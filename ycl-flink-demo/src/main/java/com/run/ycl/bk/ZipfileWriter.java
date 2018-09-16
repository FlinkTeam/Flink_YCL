package com.run.ycl.bk;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipfileWriter<T> implements Writer<T> {
    private int lineCount;
    private transient ZipOutputStream outStream;
    FSDataOutputStream fsDataOutputStream;
    byte[] indexTemplate;
    String protocol;
    String filename;

    private boolean syncOnFlush;


    public ZipfileWriter(String protocol) {
        this.protocol = protocol;
    }

    protected ZipfileWriter(ZipfileWriter<T> other) {
        this.protocol = other.protocol;
        this.syncOnFlush = other.syncOnFlush;
    }

    public void setSyncOnFlush(boolean syncOnFlush) {
        this.syncOnFlush = syncOnFlush;
    }

    public void getIndexTemplate() {
        List<FieldProperties> fieldList = ProtocolFields.protocolFieldsMap.get(protocol);
        indexTemplate = BKConfigInit.getDocument(protocol, filename, lineCount, fieldList);
    }

    @Override
    public void write(T element) throws IOException {
        ZipOutputStream outputStream = getStream();
        outputStream.write(element.toString().getBytes());
        lineCount++;
    }

    @Override
    public Writer<T> duplicate() {
        return new ZipfileWriter<>(this);
    }

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        if (outStream != null) {
            throw new IllegalStateException("Writer has already been opened");
        }
        //filename = path.getName();
        String name = path.getName();
        int pos = name.lastIndexOf(".zip");
        String subtaskAndCount = name.substring(pos - 3, pos);
        long currentTime = System.currentTimeMillis()/1000;
        filename = String.format("%s-%s-%s-%s-%s-%s.bcp", 137, "010000", currentTime, "010000", protocol, subtaskAndCount);
        ///Path pathNew = new Path(path.getParent(), String.format("%s-%s-%s-%s-%s-%s",137,"705420347","010000","010000",currentTime,name.substring(pos-3)));
        fsDataOutputStream = fs.create(path, false);
        outStream = new ZipOutputStream(fsDataOutputStream);
        ZipEntry zipEntry = new ZipEntry(filename);
        outStream.putNextEntry(zipEntry);
    }

    @Override
    public long flush() throws IOException {
        if (outStream == null) {
            throw new IllegalStateException("Writer is not open");
        }
        outStream.flush();
        if (syncOnFlush) {
            fsDataOutputStream.hsync();
        } else {
            fsDataOutputStream.hflush();
        }
        return fsDataOutputStream.getPos();
    }

    @Override
    public long getPos() throws IOException {
        if (fsDataOutputStream == null) {
            throw new IllegalStateException("Writer is not open");
        }
        return fsDataOutputStream.getPos();
    }

    protected ZipOutputStream getStream() {
        if (outStream == null) {
            throw new IllegalStateException("Output stream has not been opened");
        }
        return outStream;
    }

    @Override
    public void close() throws IOException {
        if (outStream != null) {
            flush();
            outStream.closeEntry();
            ZipEntry zipEntry = new ZipEntry("GAB_ZIP_INDEX.xml");
            outStream.putNextEntry(zipEntry);
            getIndexTemplate();
            outStream.write(indexTemplate);
            flush();
            outStream.closeEntry();
            outStream.close();
            outStream = null;
            fsDataOutputStream.close();
            fsDataOutputStream = null;
        }
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(syncOnFlush);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        ZipfileWriter<T> writer = (ZipfileWriter<T>) other;
        // field comparison
        return Objects.equals(syncOnFlush, writer.syncOnFlush);
    }
}


class NewZipfileWriter<T> implements Writer<T> {
    private String prex;
    private String subtaskidAndCount;
    private transient Map<String, OutputFileInfo> outputFileInfoMap = new HashMap<String, OutputFileInfo>();
    private FileSystem fs;
    private Path path;
    private transient ZipOutputStream outStream;
    private long size;

    private boolean syncOnFlush;

    private static final Logger logger = LoggerFactory.getLogger(NewZipfileWriter.class);

    public static class OutputFileInfo{
        private int lineCount;
        private transient FSDataOutputStream bcpOutStream;
        Path filePath;

        public int getLineCount() {
            return lineCount;
        }

        public void setLineCount(int lineCount) {
            this.lineCount = lineCount;
        }

        public FSDataOutputStream getBcpOutStream() {
            return bcpOutStream;
        }

        public void setBcpOutStream(FSDataOutputStream bcpOutStream) {
            this.bcpOutStream = bcpOutStream;
        }

        public Path getFilePath() {
            return filePath;
        }

        public void setFilePath(Path filePath) {
            this.filePath = filePath;
        }
    }


    public NewZipfileWriter(){
    }

    public void setSyncOnFlush(boolean syncOnFlush) {
        this.syncOnFlush = syncOnFlush;
    }

    protected NewZipfileWriter(NewZipfileWriter<T> other) {
        this.syncOnFlush = other.syncOnFlush;
    }

    public byte[] getIndexTemplate(Map<String, OutputFileInfo> map){
        return BKOutputIndex.getDocument(map);
    }

    @Override
   public void write(T element) throws IOException {
        getStream();
        if(element == null)
            return;
        writeData(element);
    }

    protected ZipOutputStream getStream() {
        if (outStream == null ) {
            throw new IllegalStateException("Output stream has not been opened");
        }
        return outStream;
    }
    private void writeData(T element) throws IOException {

        JSONObject jsonObject = (JSONObject)element;
        String protocol = jsonObject.getString("SOURCE_NAME");
        List<FieldProperties> list = ProtocolFields.protocolFieldsMap.get(protocol);

        if(null == protocol || null == list || list.isEmpty()) {
            System.out.println(String.format("cannot find %s's fields info !",protocol));
            return ;
        }

        byte[] bytes = getOneBcpData(jsonObject, list).getBytes();
        OutputFileInfo outputFileInfo = outputFileInfoMap.get(protocol);
        if(null == outputFileInfo){
            String filename = String.format("%s-%s-%s-%s-%s-%s.bcp",137,"010000",System.currentTimeMillis()/1000,"010000",protocol,subtaskidAndCount);
            if(logger.isInfoEnabled()){
                logger.info("BK==== open bcp {} in for write !",filename, path);
            }
            Path filePath = new Path(path.getParent(),filename);
            FSDataOutputStream bcpFileOutStream = fs.create(filePath, false);
            outputFileInfo = new OutputFileInfo();
            outputFileInfo.setFilePath(filePath);
            outputFileInfo.setLineCount(1);
            outputFileInfo.setBcpOutStream(bcpFileOutStream);
            outputFileInfoMap.put(protocol, outputFileInfo);
            bcpFileOutStream.write(bytes);
        }else {
            outputFileInfo.getBcpOutStream().write(bytes);
            outputFileInfo.setLineCount(outputFileInfo.getLineCount()+1);
        }
        size += bytes.length;
    }

    private String getOneBcpData(JSONObject element, List<FieldProperties> list){

        StringBuilder stringBuilder = new StringBuilder();
        for(FieldProperties field : list) {
            String val = element.getString((field.getElemenId()));
            if(null == val)
                val="";
            stringBuilder.append(val+'\t');
        }
        stringBuilder.replace(stringBuilder.length()-1,stringBuilder.length(),"\n");
        return stringBuilder.toString();
    }


    @Override
    public Writer<T> duplicate() {
        return new NewZipfileWriter<>(this);
    }

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        this.fs = fs;
        if (outStream != null) {
            throw new IllegalStateException("Writer has already been opened");
        }

        if(logger.isInfoEnabled()){
           logger.info("BK==== open {} for write !",path);
        }
        this.path = path;
        String name = path.getName();
        int pos = name.lastIndexOf(".zip");
        subtaskidAndCount = name.substring(pos-3,pos);
        FSDataOutputStream fsDataOutputStream = fs.create(path, false);
        outStream = new ZipOutputStream(fsDataOutputStream);

    }

    @Override
    public long flush() throws IOException {
        if (outStream == null) {
            throw new IllegalStateException("Writer is not open");
        }
        for(Map.Entry<String, OutputFileInfo> entry : outputFileInfoMap.entrySet()){
            FSDataOutputStream fsDataOutputStream = entry.getValue().getBcpOutStream();
            if (syncOnFlush) {
                fsDataOutputStream.hsync();
            } else {
                fsDataOutputStream.hflush();
            }
        }
        return size;
    }

    @Override
    public long getPos() throws IOException {
        return size;
    }

    @Override
    public void close() throws IOException {
        if (outStream != null) {
            if(!outputFileInfoMap.isEmpty()){
                flush();
                for(Map.Entry<String, OutputFileInfo> entry : outputFileInfoMap.entrySet()){
                    entry.getValue().getBcpOutStream().close();
                    entry.getValue().setBcpOutStream(null);
                    ZipEntry zipEntry = new ZipEntry(entry.getValue().getFilePath().getName());
                    outStream.putNextEntry(zipEntry);
                    FSDataInputStream fsDataInputStream = fs.open(entry.getValue().getFilePath());
                    IOUtils.copyBytes(fsDataInputStream, outStream, 4096);
                    fsDataInputStream.close();
                    outStream.closeEntry();
                    fs.delete(entry.getValue().getFilePath(),false);
                }
            }
            ZipEntry zipEntry = new ZipEntry("GAB_ZIP_INDEX.xml");
            outStream.putNextEntry(zipEntry);
            outStream.write(getIndexTemplate(outputFileInfoMap));
            outputFileInfoMap.clear();
            outStream.closeEntry();
            outStream.close();
            outStream = null;

            if(logger.isInfoEnabled()){
                logger.info("BK==== close {} !",path);
            }
        }
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(syncOnFlush);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        NewZipfileWriter<T> writer = (NewZipfileWriter<T>) other;
        // field comparison
        return Objects.equals(syncOnFlush, writer.syncOnFlush);
    }
}
