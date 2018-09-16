package com.run.ycl.bk;

import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtocolFields {

    public static Map<String,List<FieldProperties>> protocolFieldsMap;
    static {
        protocolFieldsMap =
        ProtocolFields.get_protocol_fields("fieldset.xml",
               ProtocolFields.get_protocol_field_component("dataset.xml"));
    }
    public static Map<String,String> get_protocol_field_component(String xmlfile){
        Map<String, String> map = new HashMap<>();
        SAXReader reader = new SAXReader();
        try {
            InputStream inputStream = ProtocolFields.class.getClassLoader().getResourceAsStream(xmlfile);
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

    public static Map<String,List<FieldProperties>> get_protocol_fields(String xmlfile, Map<String,String> protocolMap){
        SAXReader reader = new SAXReader();
        Map<String,List<FieldProperties>> protocolFields = new HashMap<>();
        Document document = null;
        try {
            InputStream inputStream = ProtocolFields.class.getClassLoader().getResourceAsStream(xmlfile);
            document = reader.read(inputStream);
            for(Map.Entry<String,String> entry : protocolMap.entrySet()) {
                List<FieldProperties> list = new ArrayList<>();
                String [] fieldParts = entry.getValue().split(",");
                for(String fieldPart : fieldParts) {
                    List<Node> fieldList = document.selectNodes(String.format("/FieldSetFile/FieldSet[@FSID=\"%s\"]/Field", fieldPart));
                    for (Node item : fieldList) {
                        FieldProperties field = new FieldProperties();
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
