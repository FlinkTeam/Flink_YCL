package com.run.ycl.source.zip;

import com.google.common.collect.Lists;
import com.run.ycl.parser.file.IndexDesBean;
import com.run.ycl.parser.file.ZipFileDelegate;
import com.run.ycl.utils.CacheUtil;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.PositionTracker;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BcpZipEventDeserializer implements EventDeserializer {

    private static final Logger logger = LoggerFactory.getLogger(BcpZipEventDeserializer.class);

    private final Charset outputCharset;
    private Charset inputCharset;
    private volatile boolean isOpen;

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT = "UTF-8";

    private final PositionTracker tracker;

    private String zipFileName;
    private ZipFileDelegate zipFile;

    private String password = "run1234!@#";

    private BufferedReader bcpReader;

    private final StopWatch stopWatch;

    private List<String> bcpFiles;
    private String xmlFile;

    private Iterator<String> bcpIterator;

    private String bcpName;

    private int columnNum = 1;

    public BcpZipEventDeserializer(Context context, File file, PositionTracker tracker, Map<String, List<String>> items,
                                   Charset inputCharset) throws IOException, ZipException {
        this.zipFileName = file.getName();
        this.zipFile = new ZipFileDelegate(file);
        this.isOpen = true;
        this.outputCharset = Charset.forName(context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
        this.tracker = tracker;
        this.stopWatch = StopWatch.createStarted();
        this.inputCharset = inputCharset;
        init();
    }

    private void init() throws IOException, ZipException {
        logger.info("Flume begin to import zipfile {}", zipFile.getZipFile().getFile().getName());
        if (zipFile.isEncrypted()) {
            zipFile.setPassword(password);
        }

        bcpFiles = new ArrayList<>();
        for (String fileName : zipFile.getFileNames()) {
            if (fileName.endsWith(".bcp")) {
                bcpFiles.add(fileName);
            } else if (fileName.endsWith(".xml")) {
                xmlFile = fileName;
                praseIndexXML();
            }
        }
        logger.info("the bcpFiles list is {}", bcpFiles);
        bcpIterator = bcpFiles.iterator();
        nextBcpReader();
    }

    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        Optional<String> event = doReadEvent();
        if (logger.isDebugEnabled()) {
            logger.info("readEvent result: {}", event.isPresent());
        }

        if (event.isPresent()) {
            Event eventW = EventBuilder.withBody(event.get().getBytes());
            eventW.getHeaders().put("bcp_Name", bcpName);
            eventW.getHeaders().put("xml_Name", xmlFile);
            eventW.getHeaders().put("columnNum", String.valueOf(columnNum));

            if (logger.isDebugEnabled()) {
                eventW.getHeaders().forEach((k,v) -> {
                    logger.debug("===readEvent===bcp_Name:{},xml_Name:{},columnNum:{}",
                            new String[]{bcpName, xmlFile, String.valueOf(columnNum)});
                });
            }

            return eventW;
        }
        
        logger.warn("event is null from : {}", bcpName);
        return null;
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            } else {
                break;
            }
        }
        return events;
    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            reset();
            tracker.close();
            // in.close();
            isOpen = false;

            stopWatch.stop();
            long seconds = stopWatch.getTime(TimeUnit.SECONDS);
            logger.info("Flume import file {} over! Spent {} seconds", zipFile.getZipFile().getFile().getName(),
                    seconds);
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    private Optional<String> doReadEvent() throws IOException {

        if (bcpReader == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("doReadEvent line:{}", 1);
            }
            return Optional.empty();
        }

        String line = readLine();
        if (StringUtils.isBlank(line)) {
            nextBcpReader();

            if (logger.isDebugEnabled()) {
                logger.debug("doReadEvent line:{}", 2);
            }
            return doReadEvent();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("doReadEvent line:{}", line);
        }
        return Optional.of(line);
    }

    private String readLine() throws IOException {
        columnNum ++;
        String line = bcpReader.readLine();
        if (logger.isDebugEnabled()) {
            logger.debug("******readLine:{}", line);
        }
        return line;
    }
    private void nextBcpReader() throws IOException {
        if (bcpIterator.hasNext()) {
            bcpName = bcpIterator.next();
            if(bcpReader != null) {
        		bcpReader.close();
        		bcpReader = null;
            }
            bcpReader = new BufferedReader(new InputStreamReader(zipFile.getFileInputStream(bcpName), inputCharset));
            columnNum = 0;
            logger.info("Flume begin to import bcpfile {}", bcpName);
        } else {
        	if(bcpReader != null) {
        		bcpReader.close();
        		bcpReader = null;
            }
        }
    }

    private void praseIndexXML() {
        if (CacheUtil.xmlFileNameCache.getIfPresent(zipFileName + xmlFile) != null) {
            return;
        }

        try {
            SAXReader reader = new SAXReader();
            Document document = reader.read(zipFile.getFileInputStream(xmlFile));
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
                CacheUtil.indexCache.put(xmlFile + indexDes.getFileName(), indexDes);
            }
            CacheUtil.xmlFileNameCache.put(zipFileName + xmlFile, "true");
                logger.info("indexCacheMap");
                CacheUtil.indexCache.asMap().forEach((k, v) -> {
                    logger.info("===k:{}====v:{}", k, v);
                });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}
