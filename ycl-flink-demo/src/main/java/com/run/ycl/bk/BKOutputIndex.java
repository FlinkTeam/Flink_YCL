package com.run.ycl.bk;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BKOutputIndex {

    public static Document documentTemplate;
    static {
        indexTemplateInit();
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

    public static byte[] getDocument(Map<String, NewZipfileWriter.OutputFileInfo> outputFileInfoMap){

        Document document = copyDocument(documentTemplate.getRootElement());

        String bcpInfoXpath="/MESSAGE/DATASET/DATA/DATASET[@name=\"WA_COMMON_010013\"]/DATA";
        String protocolXpath="./ITEM[@key=\"A010004\"]";
        String filenameXpath="./DATASET[@name=\"WA_COMMON_010014\"]/DATA/ITEM[@key=\"H010020\"]";
        String linecountXpath="./DATASET[@name=\"WA_COMMON_010014\"]/DATA/ITEM[@key=\"I010034\"]";
        String fieldcntXpath="./DATASET[@name=\"WA_COMMON_010015\"]";
        String fieldsXpath="./DATASET[@name=\"WA_COMMON_010015\"]/DATA";

        List<Element> bcpInfo = document.selectNodes(bcpInfoXpath);
        Element data = bcpInfo.get(0).createCopy();
        Element dataSet = bcpInfo.get(0).getParent();
        dataSet.remove(bcpInfo.get(0));

        for(Map.Entry<String, NewZipfileWriter.OutputFileInfo> entry : outputFileInfoMap.entrySet()){

            data = data.createCopy();
            String protocolName = entry.getKey();
            List<FieldProperties> fieldList = ProtocolFields.protocolFieldsMap.get(protocolName);
            if(null == fieldList || fieldList.isEmpty()) {
                System.out.println("ERROR: cannot find protocol fields!!");
                continue;
            }
            int lineCount = entry.getValue().getLineCount();
            String fileName = entry.getValue().getFilePath().getName();

            List<Element> protocol = data.selectNodes(protocolXpath);
            List<Element> filename = data.selectNodes(filenameXpath);
            List<Element> linecount = data.selectNodes(linecountXpath);
            List<Element> fieldcnt = data.selectNodes(fieldcntXpath);
            List<Element> fields = data.selectNodes(fieldsXpath);
            protocol.get(0).attribute("val").setValue(protocolName);
            filename.get(0).attribute("val").setValue(fileName);
            linecount.get(0).attribute("val").setValue(String.valueOf(entry.getValue().getLineCount()));
            fieldcnt.get(0).attribute("fieldcnt").setValue(String.valueOf(fieldList.size()));

            Element parent = fields.get(0).getParent();
            parent.remove(fields.get(0));
            Element addElement = parent.addElement("DATA");
            for (FieldProperties item : fieldList) {
                Element element = addElement.addElement("ITEM");
                element.addAttribute("key",item.getElemenId());
                element.addAttribute("eng",item.getEnname());
                element.addAttribute("chn",item.getChname());
                element.addAttribute("rmk","");
            }
            dataSet.add(data);
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

    public static void main(String[] args) throws IOException {

          Map<String, NewZipfileWriter.OutputFileInfo> outputFileInfoMap = new HashMap<>();
          for(String protocol: ProtocolFields.protocolFieldsMap.keySet()){

            Path filePath = new Path("a","b");

            NewZipfileWriter.OutputFileInfo outputFileInfo = new NewZipfileWriter.OutputFileInfo();
            outputFileInfo.setFilePath(filePath);
            outputFileInfo.setLineCount(1);
            outputFileInfo.setBcpOutStream(null);

            outputFileInfoMap.put(protocol, outputFileInfo);
        }

        System.out.write(BKOutputIndex.getDocument(outputFileInfoMap));

    }
}
