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
 * 非标准的CSV输入数据拦截器
 */
public class NonStandardCsvInterceptor implements Interceptor{
    private static final Logger logger = LoggerFactory.getLogger(NonStandardCsvInterceptor.class);

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

            String file = headers.get("file");
            String basename = headers.get("basename");
            String[] fileInfos = file.split("/");
            String dataSource = ""; //数据来源
            String sourceName = ""; //协议名称
            for (String fileInfo: fileInfos) {
                if(logger.isDebugEnabled()) {
                    logger.debug("before convert,fileInfo:{}", fileInfo);
                }
                if (fileInfo.startsWith("source") && !fileInfo.endsWith(".csv")) {
                    dataSource = fileInfo;
                } else if (fileInfo.endsWith(".csv")) {
                    //shunfeng_33475891_20161123-DELIVERY_SF-9.bcp
                    //根据数据来源名称，从配置中获取协议名称在文件名称中的位置
                    //暂定取最后一个，后面修改为根据配置获取
                    String dataSourceConfigStr = CacheUtil.getIfPresentFromCommonString("config_" + dataSource);
                    JSONObject dataSourceJson = JSONObject.parseObject(dataSourceConfigStr);
                    String fileInfoConfig = dataSourceJson.getString("Value");
                    if(logger.isDebugEnabled()) {
                        logger.debug("convert ing...,fileInfoConfig:{}", fileInfoConfig);
                    }
                    if (StringUtils.isNotBlank(fileInfoConfig)) {
                        String[] fileConfigInfo = fileInfoConfig.split(";");
                        for (String fileConf: fileConfigInfo) {
                            if (fileConf.startsWith("fileNameInfo")) {
                                String[] fileNameInfo = fileConf.split(":")[1].split("\\|");
                                String fileSeparator, timePosition, sourcePosition;
                                if (fileNameInfo.length > 2) {
                                    fileSeparator = fileNameInfo[0];
                                    timePosition = fileNameInfo[1];
                                    sourcePosition = fileNameInfo[2];

                                    String[] nameSplit = fileInfo.split(fileSeparator);

                                    int len = nameSplit.length;
                                    Integer source_index = Integer.valueOf(sourcePosition) - 1;
                                    //如果填写不对，默认在首部
                                    if (source_index < 0 || source_index > len - 1){
                                        logger.info("sourcePosition is error");
                                        source_index = 0;
                                    }
                                    sourceName =  nameSplit[source_index];
                                } else {
                                    logger.error("conver error, cannot get datasource name.fileInfoConfig:{}", fileInfoConfig);
                                }
                            }
                        }
                    } else {
                        logger.error("conver error, cannot get datasource name.fileInfoConfig：{}", fileInfoConfig);
                    }
                }
            }

            IndexDesBean indexDesBean = CacheUtil.getIfPresentFromNonstandardBcpIndex(dataSource, sourceName);
            //找不到映射文件的输出错误日志
            if (indexDesBean == null) {
                logger.error("conver error, cannot find index file. dataSource：{}, sourceName:{}, basename:{}, column:{}, content:{}",
                        new String[]{dataSource, sourceName, basename, headers.get("columnNum"), new String(event.getBody())});
                return null;
            }

            JSONObject json = null;
            String[] bodyContents = new String(event.getBody()).split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", 65535);

            String[] bodyItems = indexDesBean.getItems();

            if(logger.isDebugEnabled()) {
                logger.debug("before convert,bodyContents.length:{}=bodyItems.length:{}==", bodyContents.length, bodyItems.length);
            }

            if (bodyContents.length == bodyItems.length) {
                json = new JSONObject();
                json.put("SOURCE_NAME", indexDesBean.getdDataset());
                json.put("SDATASET", indexDesBean.getsDataset());
                json.put("DATA_SOURCE", dataSource);
                int i = 0;
                while(i < bodyContents.length && bodyContents.length > 0) {
                    json.put(bodyItems[i], bodyContents[i]);
                    i++;
                }

                if(logger.isDebugEnabled()) {
                    logger.debug("========jsonObject:{}", json.toJSONString());
                }

                JSONEvent jsonEvent = new JSONEvent();
                jsonEvent.setBody(json.toJSONString().getBytes());
                jsonEvent.setHeaders(headers);
                String kafkaTopic = KafkaTopicUtil.getTopic(json);
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
                logger.error("conver error, cannot find index file. dataSource：{}, sourceName:{}, basename:{}, column:{}, content:{}",
                        new String[]{dataSource, sourceName, basename, headers.get("columnNum"), new String(event.getBody())});
                return null;
            }
        } catch (Exception e) {
            logger.error("intercept error! event value{}", new String(event.getBody()), e);
        }
        return event;
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
            return new NonStandardCsvInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
