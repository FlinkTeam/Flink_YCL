package com.run.ycl.keyby;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.common.ConfConstants;
import com.run.ycl.utils.JsonUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;

/**
 * 分区.
 */
public class DataKeySelector implements KeySelector<JSONObject,String> {
    private static final Logger LOG = LoggerFactory.getLogger(DataKeySelector.class);

    private HashMap<String, String> dataSet;

    public DataKeySelector() {
    }
    public DataKeySelector(HashMap<String, String> dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public String getKey(JSONObject value) throws Exception {
        String sourceName = value.getString(ConfConstants.SOURCE_NAME);

        if (dataSet != null && dataSet.size() > 0) {
            String sourceStr = dataSet.get(sourceName);
            JSONObject dataJson = JSONObject.parseObject(sourceStr);
            JSONObject dimensions = dataJson.getJSONObject(ConfConstants.UNIQUE_CONFIG_DIMENSIONS);
            //JSONObject statResults = dataJson.getJSONObject(ConfConstants.UNIQUE_CONFIG_DIMENSIONS);
            JSONArray dimension= JsonUtils.getJSONArray(dimensions, ConfConstants.UNIQUE_CONFIG_DIMENSION);
            Iterator<Object> iterator = dimension.iterator();
            //去重的字段值
            String keyVal = "";
            //提取配置文件内容
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                String elements = jsonObject.getString(ConfConstants.UNIQUE_CONFIG_DIMENSION_ELEMENT);
                keyVal += value.getString(elements);
            }
            if (Log.isDebugEnabled()) {
                LOG.debug("去重字段值：{}", keyVal);
            }
            return keyVal;
        }
        LOG.error("DataKeySelector去重配置dataSet为{}", dataSet);
        return null;
    }
}
