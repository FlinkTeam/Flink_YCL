package com.run.ycl.filter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.DmUtils;
import com.run.ycl.utils.DvUtils;
import com.run.ycl.utils.JsonUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Iterator;

/**
 * 数据清洗
 */
public class CommonDataInvalidateFilter implements FilterFunction<JSONObject> {

    private String redisHost = "192.168.251.73";
    private CommonDataValidateFilter commonDataValidateFilter = new CommonDataValidateFilter();

    @Override
    public boolean filter(JSONObject valueJson) throws Exception {
        return !commonDataValidateFilter.filter(valueJson);
    }
    private String getString(String key) {
        return commonDataValidateFilter.toString();
    }

}
