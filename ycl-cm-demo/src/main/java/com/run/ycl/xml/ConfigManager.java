package com.run.ycl.xml;

import com.run.ycl.xml.utils.JsonUtils;
import com.run.ycl.xml.utils.RedisUtils;
import com.run.ycl.xml.utils.XMLParaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class ConfigManager {

    private static String rootPath = "F:\\config";

    public static void main(String[] args) {
        System.out.println(System.getProperty("user.dir"));

        //保存dataset、fieldset文件相关内容到redis中
        String dataPath = rootPath + File.separator + "dataset.xml";
        String fieldPath = rootPath + File.separator + "fieldset.xml";
        if (checkFileName(dataPath, "dataset.xml")
                && checkFileName(fieldPath, "fieldset.xml")){
            initfields(dataPath, fieldPath);
        }

        //系统配置文件加载
        String uniqueXml = rootPath + File.separator + "ycl_config.xml";
        Map<String, String> config = uploadConfigXml(uniqueXml);
        if (checkFileName(uniqueXml, "ycl_config.xml")){
            saveMapToRedis(config);
        }
        //去重unqiue配置文件加载
        String unique = rootPath + File.separator + "Unique.xml";
        if (checkFileName(unique, "Unique.xml")){
            Map<String, String> uniqueMap = uniqueConfigXml(unique);
            saveMapToRedis(uniqueMap);
        }


        //初始化不同数据来源的dataMap配置
        initDataMap();

        //初始化不同数据来源的run_standard协议数据转换映射
        initRunStandard();

        //协议数据集字段说明
        initRunIndex();

        //DataValidateXML加载
        String dataValidataXmlPath = rootPath + File.separator + "DataValidate.xml";
        Map<String, String> dataValidataMap = uploadDataValidataXml(dataValidataXmlPath);
        saveMapToRedis(dataValidataMap);

        //NormalizingXML加载
        String normalizingPath = rootPath + File.separator + "Normalizing.xml";
        Map<String, String> normalizingMap = uploadNormalizingXml(normalizingPath);
        saveMapToRedis(normalizingMap);

        //结构化提取Struct_Extract.xml加载
        String structExtractPath = rootPath + File.separator + "Struct_Extract.xml";
        Map<String, String> structExtractMap = uploadStructExtractXml(structExtractPath);
        saveMapToRedis(structExtractMap);
        System.out.println("end");

        //创建mysql表
       createTablesNew(XMLParaseUtils.get_protocol_fields("fieldset.xml",
               XMLParaseUtils.get_protocol_field_component("dataset.xml")));

        //将规则文件导入mysql
        /*String ruleXml = "config\\PhoneMap.xml";
        importRuleToMysql(ruleXml);*/

        //加载规则配置到redis
        /*String phoneRule = "config\\PhoneRule.xml";
        Map<String, String> phoneRuleConfig = uploadPhoneRuleXml(phoneRule);
        saveMapToRedis(phoneRuleConfig);*/
    }


    private static boolean checkFileName(String filePath, String fileName) {
        boolean flag = false;
        if (filePath != null && fileName != null) {
            File file = new File(filePath);
            if (file.exists() && file.isFile() && file.getName().equals(fileName)) {
                flag = true;
            }
        }
        return flag;
    }

    private static void initfields(String dataPath, String fieldPath) {

        JSONObject xmlJson = XMLParaseUtils.xml2jsonObject(dataPath, "utf-8");
        Map<String,String> protocolMap = new HashMap<>();

        JSONArray datasets = xmlJson.getJSONObject("DataSetFile").getJSONArray("DataSet");

        datasets.forEach(json -> {
            JSONObject dataset = (JSONObject) json;

            String dsid = dataset.getString("DSID");
            String fieldSets = dataset.getString("FieldSets");

            protocolMap.put(dsid, fieldSets);
        });

        JSONObject jsObj = new JSONObject();
        SAXReader reader = new SAXReader();
        Document document = null;
        try {
            InputStream inputStream = new FileInputStream(fieldPath);
            document = reader.read(inputStream);
            for(Map.Entry<String,String> entry : protocolMap.entrySet()) {
                String protocol = entry.getKey();
                List<Field> list = new ArrayList<>();
                String [] fieldParts = entry.getValue().split(",");

//                JSONArray fieldJarr = new JSONArray();
                JSONObject jsObjFileds = new JSONObject();
                for(String fieldPart : fieldParts) {
                    List<Node> fieldList = document.selectNodes(String.format("/FieldSetFile/FieldSet[@FSID=\"%s\"]/Field", fieldPart));
                    for (Node item : fieldList) {
                        //<Field BeAnalyzed="false" BeMerge="false" BeMultiValue="false" BeNotNull="false"
                        // BeQueried="true" CHName="手机取证采集目标编号" CodeSet="" Description=""
                        // ENName="COLLECT_TARGET_ID" ElementID="I050008" SeqNumber="1" ValueDefault=""
                        // ValueLength="57" ValueType="string"/>
                        JSONObject fieldJson = new JSONObject();
                        fieldJson.put("CHName", item.valueOf("@CHName"));
                        fieldJson.put("ENName", item.valueOf("@ENName"));
                        fieldJson.put("BeNotNull", item.valueOf("@BeNotNull"));
                        fieldJson.put("ValueLength", item.valueOf("@ValueLength"));
                        fieldJson.put("BeQueried", item.valueOf("@BeQueried"));
                        jsObjFileds.put(item.valueOf("@ElementID"),fieldJson);
                    }
                }
                jsObj.put(protocol, jsObjFileds);
            }
            HashMap<String, String> map = new HashMap<>();
            map.put("DATASET_FIELDSET", jsObj.toString());
            saveMapToRedis(map);

            Jedis jedis = RedisUtils.getRedisClient();
            String datasets1 = jedis.get("DATASET_FIELDSET");
            com.alibaba.fastjson.JSONObject parse = (com.alibaba.fastjson.JSONObject)com.alibaba.fastjson.JSONObject.parse(datasets1);
            System.out.println("..."+parse);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //区分不同的数据源dataMappingXML加载 DataMapping.xml
    private static void initDataMap() {
        String dataMappingPath = rootPath + File.separator +"datamap";
        File baseDataMapFile = new File(dataMappingPath);
        File[] dataMapSourceFlies = baseDataMapFile.listFiles();

        if (null == dataMapSourceFlies && dataMapSourceFlies.length < 1) {
            return;
        };

        for (File file : dataMapSourceFlies) {
            String dataSource = file.getName();
            File[] dataMapFlies = file.listFiles();
            if (StringUtils.isNotBlank(dataSource) && dataSource.toUpperCase().startsWith("SOURCE")) {
                for (File dmFile: dataMapFlies) {
                    if (checkFileName(dmFile.getPath(), "DataMapping.xml")) {
                        Map<String, String> dataMappingMap = uploadDataMappingXml(dataSource, dmFile.getPath());
                        saveMapToRedis(dataMappingMap);
                    }
                }
            }
        }
    }

    //初始化不同数据来源的run_standard协议数据转换映射
    private static void initRunStandard() {
        String protoolPath = rootPath + File.separator +"protocol";
        File baseStandardFile = new File(protoolPath);
        File[] standardSourceFlies = baseStandardFile.listFiles();

        if (null == standardSourceFlies && standardSourceFlies.length < 1) {
            return;
        };

        for (File file : standardSourceFlies) {
            String dataSource = file.getName();
            File[] dataMapFlies = file.listFiles();
            if (StringUtils.isNotBlank(dataSource) && dataSource.toUpperCase().startsWith("SOURCE")) {
                for (File standardFile: dataMapFlies) {
                    if (checkFileName(standardFile.getPath(), "run_standard.xml")) {
                        Map<String, String> in2OutMap = uploadStandardIn2Out(dataSource, standardFile.getPath());
                        saveMapToRedis(in2OutMap);
                    }
                }
            }
        }
    }

    //协议数据集字段说明
    private static void initRunIndex() {
        String indexPath = rootPath + File.separator +"index";
        File baseIndexFile = new File(indexPath);
        File[] indexSourceFlies = baseIndexFile.listFiles();

        if (null == indexSourceFlies && indexSourceFlies.length < 1) {
            return;
        };

        for (File file : indexSourceFlies) {
            String dataSource = file.getName();
            File[] indexFlies = file.listFiles();
            if (StringUtils.isNotBlank(dataSource) && dataSource.toUpperCase().startsWith("SOURCE")) {
                for (File indexdFile: indexFlies) {
                    if (checkFileName(indexdFile.getPath(), "run_index.xml")) {
                        Map<String, String> indexMap = uploadIndexXml(dataSource, indexdFile.getPath());
                        saveMapToRedis(indexMap);
                    }
                }
            }
        }
    }

    //加载StructExtractXml
    public static Map<String, String> uploadStructExtractXml(String path) {
        Map extractorMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("PolicyFile");
        JSONArray jsonArray = null;

        try {
            jsonArray = JsonUtils.getJSONArray(standardSource, "Extract");
            Iterator<Object>  extractIterator = jsonArray.iterator();

            while (extractIterator.hasNext()) {
                JSONObject extractorData = (JSONObject) extractIterator.next();
                extractorMap.put("Extractor_" + extractorData.getString("SDataSet"), extractorData.toString());
            }

        } catch (JSONException exception) {
            exception.printStackTrace();
        }

//        System.out.println(extractorMap.toString());
        return extractorMap;
    }

    //加载NormalizingXml
    public static Map<String, String> uploadNormalizingXml(String path) {
        Map normalizingMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("PolicyFile");
        JSONArray jsonArray = null;

        try {
            jsonArray = standardSource.getJSONArray("Normalizing");
            Iterator<Object>  normalizingIterator = jsonArray.iterator();
            //处理CodeMap
            while (normalizingIterator.hasNext()) {
                JSONObject normalizingdata = (JSONObject) normalizingIterator.next();
                normalizingMap.put("Normalizing_" + normalizingdata.getString("DataSet"), normalizingdata.toString());
            }
        } catch (JSONException exception) {
            //解决只有一个item时，xml转JsonObject时，Item被转成JsonObject
            JSONObject normalizingdata = standardSource.getJSONObject("Normalizing");;
            normalizingMap.put("Normalizing_" + normalizingdata.getString("DataSet"), normalizingdata.toString());
        }

//        System.out.println(normalizingMap.toString());
        return normalizingMap;
    }

    //加载DataValidateXML
    public static Map<String, String> uploadDataValidataXml(String path) {
        Map dataValidataMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("PolicyFile");
        JSONArray jsonArray = null;

        try {
            jsonArray = standardSource.getJSONArray("Normalizing");
            Iterator<Object>  normalizingIterator = jsonArray.iterator();
            //处理CodeMap
            while (normalizingIterator.hasNext()) {
                JSONObject normalizingdata = (JSONObject) normalizingIterator.next();
                dataValidataMap.put("DataValidata_" + normalizingdata.getString("DataSet"), normalizingdata.toString());
            }
        } catch (JSONException exception) {
            //解决只有一个item时，xml转JsonObject时，Item被转成JsonObject
            JSONObject normalizingdata = standardSource.getJSONObject("Normalizing");;
            dataValidataMap.put("DataValidata_" + normalizingdata.getString("DataSet"), normalizingdata.toString());
        }

//        System.out.println(dataValidataMap.toString());
        return dataValidataMap;
    }

    //加载dataMappingXML
    public static Map<String, String> uploadDataMappingXml(String dataSource, String path) {
        Map dataMappingMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("PolicyFile");
        JSONArray jsonArray = null;

        if (standardSource.has("CodeMap")) {
            try {
                jsonArray = JsonUtils.getJSONArray(standardSource, "CodeMap");
                Iterator<Object>  codeMapIterator = jsonArray.iterator();
                //处理CodeMap
                while (codeMapIterator.hasNext()) {
                    JSONObject codeMapdata = (JSONObject) codeMapIterator.next();
                    dataMappingMap.put(dataSource.toUpperCase() + "_Data_" + codeMapdata.getString("Name"), codeMapdata.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (standardSource.has("Normalizing")) {
            try {
                jsonArray = JsonUtils.getJSONArray(standardSource, "Normalizing");
                Iterator<Object>  normalizingIterator = jsonArray.iterator();
                //处理Normalizing
                while (normalizingIterator.hasNext()) {
                    JSONObject normalizingdata = (JSONObject) normalizingIterator.next();
                    dataMappingMap.put(dataSource.toUpperCase() + normalizingdata.getString("SDataSet") + "_2_" + normalizingdata.getString("DDataSet"), normalizingdata.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return dataMappingMap;
    }

    //加载输入输出数据集说明
    public static Map<String, String> uploadIndexXml(String dataSource, String path) {
        Map indexMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("MESSAGE").getJSONObject("DATASET").getJSONObject("DATA").getJSONObject("DATASET");
        JSONArray jsonArray = null;
        try {
            jsonArray = JsonUtils.getJSONArray(standardSource, "DATA");
            Iterator<Object>  iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                //data表示每个数据协议的说明 A010004(数据集代码)
                JSONObject data = (JSONObject) iterator.next();

                Iterator<Object> itemIterator = data.getJSONArray("ITEM").iterator();
                String a010004 = "";
                while (itemIterator.hasNext()) {
                    JSONObject item = (JSONObject) itemIterator.next();
                    if (item.optString("key", "").equals("A010004")) {
                        a010004 = item.getString("val");
                    }
                }
                indexMap.put(dataSource.toUpperCase() + "_index_des_" + a010004.toUpperCase(), data.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return indexMap;
    }

    //加载输入输出映射关系
    public static Map<String, String> uploadStandardIn2Out(String dataSource, String path) {
        Map in2outMap = new HashMap();
        //输入输出映射关系描述
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("PolicyFile").getJSONObject(dataSource);
        JSONArray jsonArray = null;
        try {
            jsonArray = JsonUtils.getJSONArray(standardSource, "Item");
            Iterator<Object>  iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                String dataSrc = jsonObject.getString("DataSrc");
                String dataOut = jsonObject.getString("DstOut");

                System.out.println("src:" + dataSrc + "======des:" + dataOut);
                in2outMap.put(dataSource.toUpperCase() + "_in2out_" + dataSrc.toUpperCase(), dataOut.toUpperCase());
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        System.out.println(in2outMap.toString());
        return in2outMap;
    }
    private static void saveMapToRedis(Map<String, String> map) {
//        System.out.println(map.toString());
        Jedis jedis = RedisUtils.getRedisClient();
        map.forEach((String k, String v) -> {
            jedis.set(k, v);
        });
        jedis.close();
    }

    private static void createTables(Map<String, String> map){
        Connection conn = null;
        Statement stmt = null;
        if(map == null || map.isEmpty()) {
            System.err.println("map is null or empty !!");
            return;
        }
        try {
            Class.forName("com.mysql.jdbc.Driver");
            try {
                conn = DriverManager.getConnection("jdbc:mysql://192.168.251.73/testdb","root","root");
                stmt = conn.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder stringBuilder2 = new StringBuilder();
        StringBuilder stringBuilder3 = new StringBuilder();
        Jedis jedis = RedisUtils.getRedisClient();
        for(Map.Entry<String,String> entry : map.entrySet()){
            String protocol = entry.getKey().replace("index_des_","");
            String value = entry.getValue();
            com.alibaba.fastjson.JSONObject jsonObject = com.alibaba.fastjson.JSONObject.parseObject(value);
            com.alibaba.fastjson.JSONArray jsonArray = jsonObject.getJSONArray("DATASET");
            Iterator<Object> iterator = jsonArray.iterator();
            while(iterator.hasNext()){
                com.alibaba.fastjson.JSONObject item = (com.alibaba.fastjson.JSONObject)(iterator.next());
                if(item.getString("name").equals("WA_COMMON_010015")){
                    Iterator<Object> itemIterator = item.getJSONObject("DATA").getJSONArray("ITEM").iterator();
                    //stringBuilder.append(String.format("create table if not exists %s (",protocol)) ;
                    //stringBuilder2.append(String.format("insert into %s (",protocol)) ;
                    stringBuilder3.append(" values (");
                    while(itemIterator.hasNext()){
                        com.alibaba.fastjson.JSONObject item2 = (com.alibaba.fastjson.JSONObject)(itemIterator.next());
                        String key = item2.getString("key");
                        String name = item2.getString("name");
                        String val = item2.getString("val");
                        stringBuilder.append(String.format("%s text comment '%s',",key,val));
                        stringBuilder2.append(key +",");
                        stringBuilder3.append("?,");
                    }
                    String createTableSql = String.format("create table if not exists %s (%s", protocol, stringBuilder.replace(stringBuilder.length()-1, stringBuilder.length(),")"));
                    String createErrTableSql = String.format("create table if not exists ERR_%s (%s", protocol, stringBuilder.replace(stringBuilder.length()-1, stringBuilder.length(),")"));
                    String insertSql = String.format("insert into %s (%s%s", protocol, stringBuilder2.replace(stringBuilder2.length()-1, stringBuilder2.length(),")"), stringBuilder3.replace(stringBuilder3.length()-1, stringBuilder3.length(),")"));
                    String insertErrSql = String.format("insert into ERR_%s (%s%s", protocol, stringBuilder2.replace(stringBuilder2.length()-1, stringBuilder2.length(),")"),stringBuilder3.replace(stringBuilder3.length()-1, stringBuilder3.length(),")"));

                    System.out.println(createTableSql);
                    System.out.println(createErrTableSql);
                    System.out.println(insertSql);
                    System.out.println(insertErrSql);
                    try {
                        stmt.executeLargeUpdate(createTableSql);
                        stmt.executeLargeUpdate(createErrTableSql);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    stringBuilder.delete(0,stringBuilder.length());
                    stringBuilder2.delete(0,stringBuilder2.length());
                    stringBuilder3.delete(0,stringBuilder3.length());
                    jedis.hset("insert_sql",protocol,insertSql);
                    jedis.hset("insert_sql_for_error_data",protocol,insertErrSql);
                }
            }
        }
        jedis.close();
        try {
            if(stmt !=null)
                stmt.close();
            if(conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTablesNew(Map<String, List<Field>> map){
        Connection conn = null;
        Statement stmt = null;
        if(map == null || map.isEmpty()) {
            System.err.println("map is null or empty !!");
            return;
        }
        try {
            Class.forName("com.mysql.jdbc.Driver");
//            try {
////                conn = DriverManager.getConnection("jdbc:mysql://192.168.251.73/testdb","root","root");
////                stmt = conn.createStatement();
//            } catch (SQLException e) {
//                e.printStackTrace();
//                return;
//            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder stringBuilder2 = new StringBuilder();
        StringBuilder stringBuilder3 = new StringBuilder();
        Jedis jedis = RedisUtils.getRedisClient();
        for(Map.Entry<String,List<Field>> entry : map.entrySet()){
            String protocol = entry.getKey();
            List<Field> fields = entry.getValue();
            stringBuilder3.append(" values (");
            for(Field field : fields) {
                String fieldElemenId = field.getElemenId();
                String fieldEnname = field.getEnname();
                String fieldChname = field.getChname();
                String fieldValueLength = field.getValueLength();
                String fieldValueType = field.getValueType();

                stringBuilder.append(String.format("%s text comment '%s',", fieldElemenId, fieldChname));
                stringBuilder2.append(fieldElemenId + ",");
                stringBuilder3.append("?,");
            }
            String createTableSql = String.format("create table if not exists %s (%s", protocol, stringBuilder.replace(stringBuilder.length()-1, stringBuilder.length(),")"));
            String createErrTableSql = String.format("create table if not exists ERR_%s (%s", protocol, stringBuilder.replace(stringBuilder.length()-1, stringBuilder.length(),")"));
            String insertSql = String.format("insert into %s (%s%s", protocol, stringBuilder2.replace(stringBuilder2.length()-1, stringBuilder2.length(),")"), stringBuilder3.replace(stringBuilder3.length()-1, stringBuilder3.length(),")"));
            String insertErrSql = String.format("insert into ERR_%s (%s%s", protocol, stringBuilder2.replace(stringBuilder2.length()-1, stringBuilder2.length(),")"),stringBuilder3.replace(stringBuilder3.length()-1, stringBuilder3.length(),")"));

            System.out.println(createTableSql);
            System.out.println(createErrTableSql);
            System.out.println(insertSql);
            System.out.println(insertErrSql);
//            try {
//                stmt.executeLargeUpdate(createTableSql);
//                stmt.executeLargeUpdate(createErrTableSql);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
            stringBuilder.delete(0,stringBuilder.length());
            stringBuilder2.delete(0,stringBuilder2.length());
            stringBuilder3.delete(0,stringBuilder3.length());
            jedis.hset("insert_sql",protocol,insertSql);
            jedis.hset("insert_sql_for_error_data",protocol,insertErrSql);
        }
        jedis.close();
        try {
            if(stmt !=null)
                stmt.close();
            if(conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //系统配置文件加载
    public static Map<String, String> uploadConfigXml(String path) {
        Map configMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("Configuration");
        JSONArray jsonArray = null;
        String dataSet = "";

        jsonArray = standardSource.getJSONArray("Module");
        Iterator<Object>  moduleIterator = jsonArray.iterator();

        while (moduleIterator.hasNext()) {
            JSONObject moduleJson = (JSONObject) moduleIterator.next();
            String moduleName = moduleJson.getString("Name");
            if ("global".equalsIgnoreCase(moduleName)) {
                try {
                    //解析全局配置
                    JSONArray globalItemsArray = JsonUtils.getJSONArray(moduleJson, "Item");
                    Iterator<Object> globalItemIterator = globalItemsArray.iterator();
                    while (globalItemIterator.hasNext()) {
                        JSONObject item = (JSONObject) globalItemIterator.next();
                        String itemName = item.getString("Name");
                        if ("dpp_city_id".equalsIgnoreCase(itemName)) {
                            configMap.put("dpp_city_id", String.valueOf(item.get("Value")));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else if ("jghqc".equalsIgnoreCase(moduleName)) {
                //结构化去重配置
                //configMap.put("UniqueStructConfig", moduleJson.toString());

            } else if (moduleName.startsWith("source_")) {
                String sourceName = moduleName;
                //来源有关的model处理
                String value = moduleJson.optString("Value");
                if (StringUtils.isNotBlank(value)) {
                    configMap.put("CONFIG_" + moduleName.toUpperCase(), moduleJson.toString());
                }
                //解析source的item
                JSONArray sourceModelItemArray = JsonUtils.getJSONArray(moduleJson, "Item");
                if (sourceModelItemArray != null) {
                    Iterator<Object> sourceModelItemIterator = sourceModelItemArray.iterator();
                    while (sourceModelItemIterator.hasNext()) {
                        JSONObject sourceModelItemJson = (JSONObject) sourceModelItemIterator.next();
                        if ("priority_topics".equalsIgnoreCase(sourceModelItemJson.getString("Name"))) {
                            //处理优先级队列相关配置
                            JSONArray priorityTopicArray = JsonUtils.getJSONArray(sourceModelItemJson, "topic");
                            if (priorityTopicArray != null) {
                                Iterator<Object> priorityTopicJsonIterator = priorityTopicArray.iterator();
                                while (priorityTopicJsonIterator.hasNext()) {
                                    JSONObject priorityTopicJson = (JSONObject) priorityTopicJsonIterator.next();
                                    String protocolName = priorityTopicJson.getString("ProtocolName");
                                    String columnName = priorityTopicJson.getString("ColumnName");
                                    String columnValue = priorityTopicJson.getString("ColumnValue");

                                    String priorityTopicsKey = "priority_topics_" + sourceName + "_" + protocolName;
                                    configMap.put(priorityTopicsKey.toUpperCase(), columnName + ";" + columnValue);
                                }
                            }
                        }
                    }
                }
            }
        }
        return configMap;
    }
    //加载去重文件
    public static Map<String, String> uniqueConfigXml(String path) {
        Map configMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("PolicyFile");
        JSONArray jsonArray = null;
        jsonArray = standardSource.getJSONArray("Unique");
        Iterator<Object>  uniqueIterator = jsonArray.iterator();
        String dataSet = "";
        int i = 0;
        while (uniqueIterator.hasNext()) {
            JSONObject uniqueJson = (JSONObject) uniqueIterator.next();
            //JSONObject dimensions = uniqueJson.getJSONObject("Dimensions");
            configMap.put("UniqueStructConfig_" + uniqueJson.getString("DataSet"), uniqueJson.toString());
            if (i++ == 0) {
                dataSet = uniqueJson.getString("DataSet");
            } else {
                dataSet += "," + uniqueJson.getString("DataSet");
            }
        }
        configMap.put("UniqueStructConfig_Interval", standardSource.get("Interval").toString());
        configMap.put("UniqueStructConfig_DataSet", dataSet);
//        System.out.println(configMap.toString());
        return configMap;
    }

    private static void importRuleToMysql(String path){
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("PolicyFile");
        JSONArray jsonArray = null;
        try {
            jsonArray = standardSource.getJSONArray("Map");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        Statement stmt = null;
        if(jsonArray == null) {
            System.err.println("map is null or empty !!");
            return;
        }
        try {
            Class.forName("com.mysql.jdbc.Driver");
            try {
                conn = DriverManager.getConnection("jdbc:mysql://192.168.251.73/rule","root","root");
                stmt = conn.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        Iterator<Object>  mapIterator = jsonArray.iterator();
        String dropTable = "drop table if exists t_phone_rule";
        String createTable = "create table  t_phone_rule" +
                "(id int(10) NOT NULL AUTO_INCREMENT,phone_pre varchar(15),op varchar(8),city varchar(15),PRIMARY KEY (id))";
        try {
            stmt.executeUpdate(dropTable);
            stmt.executeUpdate(createTable);
            while (mapIterator.hasNext()) {
                JSONObject configdata = (JSONObject) mapIterator.next();
                String phone_pre = configdata.get("Key").toString();
                String op = configdata.get("Op").toString();
                String city = configdata.get("City").toString();
                String insertSql = "insert into t_phone_rule (phone_pre,op,city)" +
                        " values(" + phone_pre + "," + op+ ","  + city + ")";
                stmt.addBatch(insertSql);

            }
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(stmt !=null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            if(conn != null)
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
    }

    //加载手机规则文件
    public static Map<String, String> uploadPhoneRuleXml(String path) {
        Map configMap = new HashMap();
        JSONObject standardJsonObject = XMLParaseUtils.xml2jsonObject(path,"utf-8");
        JSONObject standardSource = standardJsonObject.getJSONObject("Configuration");
        JSONArray jsonArray = null;
        try {
            jsonArray = standardSource.getJSONArray("Module");
            Iterator<Object>  moduleIterator = jsonArray.iterator();
            int i = 0;
            while (moduleIterator.hasNext()) {
                JSONObject configdata = (JSONObject) moduleIterator.next();
                configMap.put("PhoneRuleConfig_" + configdata.getString("DataSet"), configdata.toString());
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        System.out.println(configMap.toString());
        return configMap;
    }
}
