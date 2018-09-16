package com.run.ycl.xml.utils;

import com.run.ycl.xml.Field;
import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XMLParaseUtils {

    /**
     * 将xml文件转换成jsonString
     * @param path
     * @return
     * @throws JSONException
     * @throws IOException
     */
    public static String xml2jsonString(String path) throws JSONException, IOException {
        return xml2jsonString(path, Charset.defaultCharset().toString());
    }

    /**
     *
     * @param path
     * @param charset
     * @return
     * @throws JSONException
     * @throws IOException
     */
    public static String xml2jsonString(String path, String charset) throws JSONException {
        InputStream in = null;
        try {
            in = new FileInputStream(path);
            String xml = IOUtils.toString(in, charset);
            JSONObject xmlJSONObj = XML.toJSONObject(xml);
            return xmlJSONObj.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "";
    }

    /**
     *
     * @param path
     * @param charset
     * @return
     * @throws JSONException
     * @throws IOException
     */
    public static JSONObject xml2jsonObject(String path, String charset) throws JSONException {
        InputStream in = null;
        JSONObject xmlJSONObj = null;
        try {
            in = new FileInputStream(path);
            String xml = IOUtils.toString(in, charset);
            xmlJSONObj = XML.toJSONObject(xml);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return xmlJSONObj;
    }




    public static Map<String,String> get_protocol_field_component(String xmlfile){
        Map<String, String> map = new HashMap<>();
        SAXReader reader = new SAXReader();
        try {
            InputStream inputStream = XMLParaseUtils.class.getClassLoader().getResourceAsStream(xmlfile);
            Document document = reader.read(inputStream);
            List<Node> dataSetList = document.selectNodes("/DataSetFile/DataSet");
            for(Node dataSet : dataSetList){
                map.put(dataSet.valueOf("@DSID"),dataSet.valueOf("@FieldSets"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static Map<String,List<Field>> get_protocol_fields(String xmlfile, Map<String,String> protocolMap){
        SAXReader reader = new SAXReader();
        Map<String,List<Field>> protocolFields = new HashMap<>();
        Document document = null;
        try {
            InputStream inputStream = XMLParaseUtils.class.getClassLoader().getResourceAsStream(xmlfile);
            document = reader.read(inputStream);
            for(Map.Entry<String,String> entry : protocolMap.entrySet()) {
                List<Field> list = new ArrayList<>();
                String [] fieldParts = entry.getValue().split(",");
                for(String fieldPart : fieldParts) {
                    List<Node> fieldList = document.selectNodes(String.format("/FieldSetFile/FieldSet[@FSID=\"%s\"]/Field", fieldPart));
                    for (Node item : fieldList) {
                        Field field = new Field();
                        field.setChname(item.valueOf("@CHName"));
                        field.setEnname(item.valueOf("@ENName"));
                        field.setElemenId(item.valueOf("@ElementID"));
                        field.setBeNotNull(item.valueOf("@BeNotNull"));
                        field.setValueLength(item.valueOf("@ValueLength"));
                        list.add(field);
                    }
                }
                protocolFields.put(entry.getKey(),list);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return protocolFields;
    }


}
