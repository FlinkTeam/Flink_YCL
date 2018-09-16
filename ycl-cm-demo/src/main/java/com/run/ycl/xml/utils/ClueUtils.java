package com.run.ycl.xml.utils;



import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClueUtils {

    private static final int REDIS_DB_INDEX = 10;

    public List<ClueInfo> clueInfoList = new ArrayList<>();

    private static class Action{

        public String getActionType() {
            return actionType;
        }

        public void setActionType(String actionType) {
            this.actionType = actionType;
        }

        String actionType;
        String des;
        String fillvalue;
        String operation;

        public String getDes() {
            return des;
        }

        public void setDes(String des) {
            this.des = des;
        }

        public String getFillvalue() {
            return fillvalue;
        }

        public void setFillvalue(String fillvalue) {
            this.fillvalue = fillvalue;
        }


        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "actionType='" + actionType + '\'' +
                    ", des='" + des + '\'' +
                    ", fillvalue='" + fillvalue + '\'' +
                    ", operation='" + operation + '\'' +
                    '}';
        }
    }
    private static class ClueInfo{
        String clueId;
        String dataSet;

        List<Action> actions;
        List<List<String>> subClueInfo;

        public String getClueId() {
            return clueId;
        }

        public void setClueId(String clueId) {
            this.clueId = clueId;
        }

        public String getDataSet() {
            return dataSet;
        }

        public void setDataSet(String dataSet) {
            this.dataSet = dataSet;
        }

        public List<Action> getActions() {
            return actions;
        }

        public void setActions(List<Action> actions) {
            this.actions = actions;
        }

        public List< List<String>> getSubClueInfo() {
            return subClueInfo;
        }

        public void setSubClueInfo(List<List<String>> subClueInfo) {
            this.subClueInfo = subClueInfo;
        }

        @Override
        public String toString() {
            return "ClueInfo{" +
                    "clueId='" + clueId + '\'' +
                    ", dataSet='" + dataSet + '\'' +
                    ", actions=" + actions +
                    ", subClueInfo=" + subClueInfo +
                    '}';
        }
    }

    public void treeWalk(Document document){

        ClueInfo clueInfoNew = new ClueInfo();
        treeWalk(document.getRootElement(),clueInfoNew);
    }


    public void treeWalk(Element element,ClueInfo clueInfo){
        for(int i=0,size=element.nodeCount();i<size;i++ ) {
            Node node = element.node(i) ;
            if(node instanceof Element) {
                switch (node.getName()){
                    case "clue":
                        ClueInfo clueInfoNew = new ClueInfo();
                        treeWalk((Element)node,clueInfoNew);
                        clueInfoList.add(clueInfoNew);
                        break;
                    case  "clueid":
                        clueInfo.setClueId(node.getText().trim());
                        break;
                    case "dataset":
                        clueInfo.setDataSet(node.getText().trim());
                        break;
                    case "common":
                        List<Action> actions = new ArrayList<>();
                        for(Iterator<Element> it = ((Element) node).elementIterator("action");it.hasNext();){
                            Element elementAction= it.next();
                            Action actionTmp = new Action();
                            actionTmp.setActionType(elementAction.attributeValue("actiontype"));
                            for(Iterator<Element> it2 = elementAction.elementIterator();it2.hasNext();) {
                                Element element1 = it2.next();
                                switch (element1.getName()) {
                                    case "des":
                                        actionTmp.setDes(element1.getText().trim());
                                    case "fillvalue":
                                        actionTmp.setFillvalue(element1.getText().trim());
                                    case "operation":
                                        actionTmp.setOperation(element1.getText().trim());
                                }
                            }
                            actions.add(actionTmp);
                        }
                        clueInfo.setActions(actions);
                        break;
                    case "clueinfo":
                        List<List<String>> list = new ArrayList<>();
                        for(Iterator<Element> it = ((Element) node).elementIterator("subclue");it.hasNext();){
                            List<String> listTmp = new ArrayList<>();
                            for(Iterator<Element> it2 =  it.next().elementIterator();it2.hasNext();) {
                                Element element1 = it2.next();
                                getConditions(element1,listTmp);
                            }
                            if(!listTmp.isEmpty())
                                list.add(listTmp);
                        }
                        clueInfo.setSubClueInfo(list);
                        break;
                }
                treeWalk((Element) node,clueInfo);
            }else {
                //TODO
            }

        }
    }

   public boolean isIPv4Address(String ip) {
        String lower = "(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])";
        String regex = lower +"(\\." + lower + "){3}";
       Pattern pattern = Pattern.compile(regex);
       Matcher matcher = pattern.matcher(ip);
       return matcher.matches();
   }

   public long ip2Integer(String ip){
       if(!isIPv4Address(ip))
           throw new RuntimeException("invalid ip address");
       Pattern pattern = Pattern.compile("\\d+");
       Matcher matcher = pattern.matcher(ip);
       long result = 0;
       int counter = 0;
       while (matcher.find()){
           long value = Integer.parseInt(matcher.group());
           result = (value << 8* (3-counter++)) |result;
       }
       return  result;
   }

   public  List<String> getConditions(Element element,List<String> list){

       if (null != element){
           String op = element.attributeValue("op");
           String value = element.attributeValue("value");
           if( op != null && op.equals("btw")){
               String min = element.attributeValue("min");
               String max = element.attributeValue("max");
               if(min != null && max != null){
                   if(isIPv4Address(min) && isIPv4Address(max)) {
                       long beg = ip2Integer(min);
                       long end = ip2Integer(max);
                      for (long i = beg; i <= end; i++)
                          list.add(element.getText().trim()+"="+i);
                   }
               }
           }else if(null != value)
               list.add(element.getText().trim()+"="+value);
       }
       return list;
    }

    public void clues2redis(List<ClueInfo> clueInfoListlist){
        Jedis jedis = RedisUtils.getRedisClient();
        jedis.select(REDIS_DB_INDEX);
        long internalClueId = 0;
        jedis.incr("incrID");
        Pipeline pipeline = jedis.pipelined();
        for(ClueInfo clueInfo : clueInfoListlist){
           if(null != clueInfo && !jedis.exists(clueInfo.getClueId())) {
               for(List<String> list : clueInfo.getSubClueInfo()){
                   //jedis.resetState();
                   internalClueId = Long.valueOf(jedis.get("incrID"));
                   for(String item : list){
                       pipeline.sadd(item,String.valueOf(internalClueId));
                   }
                   String [] dataset = clueInfo.getDataSet().split(",");
                   for(String item:dataset){
                        pipeline.setbit(item,internalClueId,true);
                   }
                   pipeline.set(internalClueId+"@"+list.size(), clueInfo.getClueId());
                   pipeline.incr("incrID");
                   pipeline.sync();
               }

               StringBuilder stringBuilder = new StringBuilder();
               for(Action action : clueInfo.getActions()){
                   stringBuilder.append(String.format("%s,%s,%s,%s\t",action.getActionType(),action.getDes(),action.getFillvalue(),action.getOperation()));
               }
               pipeline.hset(clueInfo.getClueId(),"clueid",clueInfo.getClueId());
               pipeline.hset(clueInfo.getClueId(),"actions",stringBuilder.toString());
               pipeline.hset(clueInfo.getClueId(),"datasets",clueInfo.getDataSet());
            }
            pipeline.sync();
            try {
                pipeline.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        jedis.close();
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 1){
            System.out.println("usage: xxx.jar ClueUtils clue_file.xml");
        }
        Path cluesDir = Paths.get(args[0]);
        if(Files.isDirectory(cluesDir)){
            Files.walkFileTree(cluesDir,new SimpleFileVisitor<Path>(){
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Objects.requireNonNull(file);
                    Objects.requireNonNull(attrs);
                    if(attrs.isRegularFile() && file.toString().endsWith(".xml"))
                        ReadClues(file);
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    Objects.requireNonNull(file);
                    System.out.println("clue file is error: " + file );
                    throw exc;
                }
            });
        }else {
            ReadClues(cluesDir);
        }

    }

    private static void ReadClues(Path path) {
        InputStream inputStream = null;
        try {
            inputStream = Files.newInputStream(path);
            //inputStream = new FileInputStream("F:\\zhidong\\work\\ServertoRedis\\clue-new.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        SAXReader reader = new SAXReader();
        try {
            ClueUtils clueUtils = new ClueUtils();
            Document document = reader.read(inputStream);
            clueUtils.treeWalk(document);
            clueUtils.clues2redis(clueUtils.clueInfoList);

            System.out.println(clueUtils.clueInfoList.toString());

        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

}
