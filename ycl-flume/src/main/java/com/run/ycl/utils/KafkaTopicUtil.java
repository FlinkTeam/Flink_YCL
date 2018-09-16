package com.run.ycl.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by jianghui on 18-8-24.
 */
public class KafkaTopicUtil {

    //highlevel_label="ycl_hlevel_"
    //normal_label="ycl_normal.*"

    public static String getTopic(JSONObject valueJson) {
        String dataSource = valueJson.getString("DATA_SOURCE");
        String sourceName = valueJson.getString("SDATASET");

        String topicStr = "ycl_hlevel_" + dataSource + "_" + sourceName;
        String topicKey = "PRIORITY_TOPICS_" + dataSource.toUpperCase() + "_" + sourceName.toUpperCase();
        String topicConf = CacheUtil.getIfPresentFromCommonString(topicKey);

        if (StringUtils.isNotBlank(topicConf)) {
            String[] topicInfos = topicConf.split(";");
            //小协议;字段1;字段2;字段3
            //ProtocolName="DELIVERY_SF" ColumnName="B020010" ColumnValue="1478170901,1478146612"

            if (topicInfos.length == 1) {
                String columnName =  topicInfos[0];
            } else if (topicInfos.length > 1) {
                String columnName =  topicInfos[0];
                String columnValue =  topicInfos[1];

                if (StringUtils.isNotBlank(columnValue)) {
                    String[] columeValueArray = columnValue.split(",");
                    for (String val : columeValueArray) {
                        if (val.equalsIgnoreCase(valueJson.getString(columnName))) {
                            topicStr += "_" + val;
                            break;
                        }
                    }
                }
            }
        } else {
            //没有特殊配置的数据，接入到任务启动配置的default topic中
            topicStr = "";
        }
        return topicStr;
    }
}
