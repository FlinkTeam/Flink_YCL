package com.run.ycl.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.parser.file.IndexDesBean;
import com.run.ycl.utils.CacheUtil;
import com.run.ycl.utils.KafkaTopicUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 标准的BCP+zip格式输入数据拦截器
 */
public class StandardBCPInterceptor implements Interceptor{
    private static final Logger logger = LoggerFactory.getLogger(StandardBCPInterceptor.class);

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        try {
            Map<String, String> headers = event.getHeaders();
            if(logger.isDebugEnabled()) {
                headers.forEach((k,v)-> {
                    logger.debug("intercept event header===k:{}, v:{}", k, v);
                });
            }

            String zipFileName = headers.get("file");
            String xmlFileName = headers.get("xml_Name");
            String columnNum = headers.get("columnNum");
            String bcp_Name = headers.get("bcp_Name");
            String dataSource = ""; //数据来源
            String[] fileInfos = zipFileName.split("/");
            String bcpFileName;
            if (bcp_Name.contains("/")) {
                String[] bcpNames = bcp_Name.split("/");
                bcpFileName = bcpNames[1];
            } else {
                bcpFileName = bcp_Name;
            }

            //获取数据来源
            for (String fileInfo: fileInfos) {
                if(logger.isDebugEnabled()) {
                    logger.debug("before convert,fileInfo:{}", fileInfo);
                }
                if (fileInfo.startsWith("source") && !fileInfo.endsWith(".zip") && !fileInfo.endsWith(".bcp")) {
                    dataSource = fileInfo;
                }
            }
            IndexDesBean indexDesBean = CacheUtil.indexCache.getIfPresent(xmlFileName + bcpFileName);
            //找不到映射文件的输出错误日志
            if (indexDesBean == null) {
                logger.error("conver error, cannot find index file. zipFileName：{},bcpFileName:{},column:{}",
                        new String[]{zipFileName, bcpFileName, headers.get("columnNum"), new String(event.getBody())});
            }

            //数据起止行之前的数据丢弃
            if (indexDesBean.getDataBeginLineNum() > 1 && Integer.valueOf(columnNum) < indexDesBean.getDataBeginLineNum()) {
                logger.error("conver error, DataBeginLineNum ：{},columnNum:{}",
                        new String[]{indexDesBean.getDataBeginLineNum() + "", columnNum});
                return null;
            }

            JSONObject json = null;
            String[] bodyContents = new String(event.getBody()).split("\\t", 65535);
            String[] bodyItems = indexDesBean.getItems();

            if(logger.isDebugEnabled()) {
                logger.debug("before convert,bodyContents.length:{}=bodyItems.length:{}==", bodyContents.length, bodyItems.length);
            }

            if (bodyContents.length == bodyItems.length) {
                json = new JSONObject();
                json.put("SOURCE_NAME", indexDesBean.getdDataset());
                json.put("SDATASET", indexDesBean.getdDataset());
                json.put("DATA_SOURCE", dataSource);
                int i = 0;
                while(i < bodyContents.length && bodyContents.length > 0) {
                    json.put(bodyItems[i], bodyContents[i]);
                    i++;
                }

                if(logger.isDebugEnabled()) {
                    logger.debug("========jsonObject:{}", json.toJSONString());
                }
                String kafkaTopic = KafkaTopicUtil.getTopic(json);
                JSONEvent jsonEvent = new JSONEvent();
                jsonEvent.setBody(json.toJSONString().getBytes());
                jsonEvent.setHeaders(headers);
                if(logger.isDebugEnabled()) {
                    logger.debug("========kafkaTopic:{}", kafkaTopic);
                }
                if (StringUtils.isNotBlank(kafkaTopic)) {
                    //针对没有特殊配置的数据源，数据接入到默认的topic中
                    jsonEvent.getHeaders().put("topic", kafkaTopic);
                }
                return jsonEvent;
            } else {
                //错误数据输出，有待进一步完善
                logger.error("conver error, zipFileName：{},bcpFileName:{},column:{}",
                        new String[]{zipFileName, bcpFileName, headers.get("columnNum"), new String(event.getBody())});
                return null;
            }
        } catch (Exception e) {
            logger.error("intercept error! event value{}", new String(event.getBody()), e);
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> out = new ArrayList<Event>();
        for (Event event: list) {
            Event outEvent = intercept(event);
            if (event != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new StandardBCPInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
