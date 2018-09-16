package com.run.ycl.parser.file;


import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSetXmlInBcpZipParser {

    private static Map<String, IndexDesBean> indexCacheMap = new HashMap<String, IndexDesBean>();
    private static Map<String, String> xmlFileNameMap = new HashMap<String, String>();

    public static void put(String zipFilePath, String xmlFileName) {
        if (null == xmlFileNameMap.get(xmlFileName)) {
            parserXML(zipFilePath, xmlFileName);
        }
    }

    public static IndexDesBean getIndexDesBean(String xmlFileName, String key) {
        return indexCacheMap.get(xmlFileName + key);
    }

    public static void parserXML(String zipFilePath, String xmlFileName) {
        try {
            File file = new File(zipFilePath);
            ZipFileDelegate zipFile = new ZipFileDelegate(file);
            if (zipFile.isEncrypted()) {
                zipFile.setPassword("");
            }

            SAXReader reader = new SAXReader();
            Document document = reader.read(zipFile.getFileInputStream(xmlFileName));
            List<Node> dataList = document.selectNodes("/MESSAGE/DATASET/DATA/DATASET/DATA");
            for (Node dataNode : dataList) {
                IndexDesBean indexDes = new IndexDesBean();

                List<Node> dataItemList = dataNode.selectNodes("ITEM");
                for(Node dataItem : dataItemList) {
                    String value = dataItem.valueOf("@val");
                    String key = dataItem.valueOf("@key");
                    if ("A010004".equalsIgnoreCase(key)) {
                        indexDes.setdDataset(value);
                    } else if ("I010038".equalsIgnoreCase(key)) {
                        indexDes.setDataBeginLineNum(Integer.valueOf(value));
                    }
                }

                List<Node> dataSetList = dataNode.selectNodes("DATASET");
                for (Node dataSetNode: dataSetList) {
                    String name = dataSetNode.valueOf("@name");
                    if ("WA_COMMON_010014".equalsIgnoreCase(name)) {
                        //BCP文件信息
                        List<Node> bcpFileItemList = dataSetNode.selectNodes("DATA/ITEM");
                        for(Node bcpFileItem : bcpFileItemList) {
                            String value = bcpFileItem.valueOf("@val");
                            String key = bcpFileItem.valueOf("@key");
                            if ("H010020".equalsIgnoreCase(key)) {
                                indexDes.setFileName(value);
                            }
                        }
                    } else if ("WA_COMMON_010015".equalsIgnoreCase(name)) {
                        //BCP文件数据结构
                        List<Node> bcpItemList = dataSetNode.selectNodes("DATA/ITEM");
                        Integer size = bcpItemList.size();
                        if (size > 0) {
                            String[] items = new String[size];
                            int i = 0;
                            for(Node bcpItem : bcpItemList) {
                                String key = bcpItem.valueOf("@key");
                                items[i++] = key;
                            }
                            indexDes.setItems(items);
                        }
                    }
                }
                indexCacheMap.put(xmlFileName + indexDes.getFileName(), indexDes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testParaserXML() {
        String zipFilePath = "F:\\aa.zip";
        String xmlFileName = "aa\\GAB_ZIP_INDEX.xml";
        parserXML(zipFilePath, xmlFileName);
    }
}
