package com.run.ycl.bk;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.OutputTag;
import org.dom4j.*;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.*;
import java.util.*;

public class BKConfigInit {
    public static Map<String,Field> compareFieldMap = new HashMap<>();
    public static Set<String> compareDataSet = new HashSet<>();
    public static Set<String> compareDataStreamSet = new HashSet<>();
    public static Map<String,String> compareOutputStreamMap = new HashMap<>();
    public static Map<String,OutputTag<JSONObject>> bkOutputTag = new HashMap<>();
    public static Document documentTemplate;
    static {
        BKinit();
        indexTemplateInit();
    }

    public final static class Field{
        String fieldName;
        String compareMethod;
        String normal;
        String normalMethod;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getCompareMethod() {
            return compareMethod;
        }

        public void setCompareMethod(String compareMethod) {
            this.compareMethod = compareMethod;
        }

        public String getNormal() {
            return normal;
        }

        public void setNormal(String normal) {
            this.normal = normal;
        }

        public String getNormalMethod() {
            return normalMethod;
        }

        public void setNormalMethod(String normalMethod) {
            this.normalMethod = normalMethod;
        }

        @Override
        public String toString() {
            return "Field{" +
                    "fieldName='" + fieldName + '\'' +
                    ", compareMethod='" + compareMethod + '\'' +
                    ", normal='" + normal + '\'' +
                    ", normalMethod='" + normalMethod + '\'' +
                    '}';
        }
    }

    public static Map<String,Field> getCompareFieldMap() {
        return compareFieldMap;
    }

    public static void setCompareFieldMap(Map<String, Field> compareFieldSet) {
        BKConfigInit.compareFieldMap = compareFieldSet;
    }

    private static void BKinit() {
        InputStream inputStream = BKConfigInit.class.getClassLoader().getResourceAsStream("ycl_bd.xml");
        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(inputStream);
            List<Node> itemList = document.selectNodes("/Configuration/ElementSet/Item");
            List<Node> itemList2 = document.selectNodes("/Configuration/DataSet/Item/@element");
            List<Node> itemList3 = document.selectNodes("/Configuration/DataStream/Item/@element_stream");
            List<Node> itemList4 = document.selectNodes("/Configuration/systemid-outputstream/Item");
            for(Node item : itemList){
                Field field = new Field();
                field.setFieldName(item.valueOf("@element_name"));
                field.setCompareMethod(item.valueOf("@compare_method"));
                field.setNormal(item.valueOf("@normal"));
                field.setNormalMethod(item.valueOf("@normal_method"));
                compareFieldMap.put(item.valueOf("@element_name"),field);
            }
            for(Node item : itemList2){
                compareDataSet.add(item.getText());
            }
            for(Node item : itemList3){
                compareDataStreamSet.add(item.getText());
            }
            for(Node item : itemList4){
                compareOutputStreamMap.put(item.valueOf("@systemid"),item.valueOf("@outputstream"));
            }
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        getBKOutputTag(compareOutputStreamMap);
    }

    public static void indexTemplateInit(){
        InputStream inputStream = BKConfigInit.class.getClassLoader().getResourceAsStream("GAB_ZIP_INDEX.xml");
        SAXReader reader = new SAXReader();
        try {
            documentTemplate = reader.read(inputStream);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    public static Document copyDocument(Element rootElement){
        Document document = DocumentHelper.createDocument();
        document.add((Element)rootElement.createCopy());
        return document;
    }

    public static byte[] getDocument(String protocolName, String fileName, int fieldCount, List<FieldProperties> list){
        Document document = copyDocument(documentTemplate.getRootElement());

        String protocolXpath="/MESSAGE/DATASET/DATA/DATASET/DATA/ITEM[@key=\"A010004\"]";
        String filenameXpath="/MESSAGE/DATASET/DATA/DATASET/DATA/DATASET[@name=\"WA_COMMON_010014\"]/DATA/ITEM[@key=\"H010020\"]";
        String countXpath="/MESSAGE/DATASET/DATA/DATASET/DATA/DATASET[@name=\"WA_COMMON_010014\"]/DATA/ITEM[@key=\"I010034\"]";
        String fieldcntXpath="/MESSAGE/DATASET/DATA/DATASET/DATA/DATASET[@name=\"WA_COMMON_010015\"]";
        String fieldsXpath="/MESSAGE/DATASET/DATA/DATASET/DATA/DATASET[@name=\"WA_COMMON_010015\"]/DATA";

        List<Element> protocol = document.selectNodes(protocolXpath);
        List<Element> filename = document.selectNodes(filenameXpath);
        List<Element> count = document.selectNodes(countXpath);
        List<Element> fieldcnt = document.selectNodes(fieldcntXpath);
        List<Element> fields = document.selectNodes(fieldsXpath);
        protocol.get(0).attribute("val").setValue(protocolName);
        filename.get(0).attribute("val").setValue(fileName);
        count.get(0).attribute("val").setValue(String.valueOf(fieldCount));
        fieldcnt.get(0).attribute("fieldcnt").setValue(String.valueOf(list.size()));

        Element parent = fields.get(0).getParent();
        parent.remove(fields.get(0));
        Element addElement = parent.addElement("DATA");
        for (FieldProperties item : list) {
            Element element = addElement.addElement("ITEM");
            element.addAttribute("key",item.getElemenId());
            element.addAttribute("eng",item.getEnname());
            element.addAttribute("chn",item.getChname());
            element.addAttribute("rmk","");
        }
        OutputFormat format = OutputFormat.createPrettyPrint();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            XMLWriter writer = new XMLWriter(byteArrayOutputStream,format);
            writer.write(document);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static void getBKOutputTag(Map<String,String> map, Set<String> set){
        for (Map.Entry<String,String> entry : map.entrySet()){
            for(String item : set) {
                String key = String.format("%s/%s", entry.getKey(),item);
                bkOutputTag.put(key, new OutputTag<JSONObject>(key){});
            }

        }
    }
    public static void getBKOutputTag(Map<String,String> map){
        for (Map.Entry<String,String> entry : map.entrySet()){
                String key = String.format("%s", entry.getKey());
                bkOutputTag.put(key, new OutputTag<JSONObject>(key){});
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(BKConfigInit.compareFieldMap);
        System.out.println(BKConfigInit.compareDataSet);
        System.out.println(BKConfigInit.compareDataStreamSet);
        System.out.println(BKConfigInit.compareOutputStreamMap);
        OutputFormat format = OutputFormat.createPrettyPrint();
        XMLWriter xmlWriter = new XMLWriter(System.out,format);
        //xmlWriter.write(BKConfigInit.getDocument("email","abc.bcp",98,Arrays.asList(new String[]{"a","b"})));
    }

}
