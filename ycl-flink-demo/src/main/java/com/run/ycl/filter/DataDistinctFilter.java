package com.run.ycl.filter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.JsonUtils;
import com.run.ycl.utils.Md5Util;
import com.run.ycl.utils.RedisUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.util.Iterator;

/**
 * 去重.
 */
public class DataDistinctFilter extends RichFilterFunction<JSONObject> {
    private static final String SOURCE_NAME = "SOURCE_NAME";
    private static final String UNIQUE_CONFIG_PRE = "UniqueConfig_";
    private static final String UNIQUE_CONFIG_ITEM = "Item";
    private static final String UNIQUE_CONFIG_ITEM_NODE_NAME = "Name";
    private static final String UNIQUE_CONFIG_ITEM_GACODE = "dwsh_distinct_gacode";
    private static final String UNIQUE_CONFIG_ITEM_INTERVAL = "dwsh_bcpduplicate_interval";
    private static final String UNIQUE_CONFIG_ITEM_NODE_VALUE = "Value";
    private Jedis jedis = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = RedisUtils.getRedisClient();
    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }

    @Override
    public boolean filter(JSONObject value) throws Exception {

        try {
            String sourceName = value.getString(SOURCE_NAME);
            String sourceStr = jedis.get(UNIQUE_CONFIG_PRE + sourceName);
            if (sourceStr == null) {
                return true;
            }
            JSONObject dataJson = JSONObject.parseObject(sourceStr);
            JSONArray items= JsonUtils.getJSONArray(dataJson, UNIQUE_CONFIG_ITEM);
            Iterator<Object> iterator = items.iterator();
            String elements = "";
            int interval = 0;
            //提取配置文件内容
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                String nameVal = jsonObject.getString(UNIQUE_CONFIG_ITEM_NODE_NAME);
                if (UNIQUE_CONFIG_ITEM_GACODE.equals(nameVal)) {
                    elements = jsonObject.getString(UNIQUE_CONFIG_ITEM_NODE_VALUE);
                } else if (UNIQUE_CONFIG_ITEM_INTERVAL.equals(nameVal)) {
                    interval = jsonObject.getIntValue(UNIQUE_CONFIG_ITEM_NODE_VALUE);
                }
            }
            //取出每个去重字段
            String[] split = elements.split(",");
            String field = "";
            for (String e : split) {
                field += value.getString(e);
            }
            String md5 = Md5Util.getMd5(field);
            String key = sourceName + "-" + md5;
            Boolean isExists = jedis.exists(key);
            if (isExists) {
                return false;
            }
            //将指定去重字段值MD5加协议作为key，放入redis
            jedis.setex(key,interval,"");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
}
