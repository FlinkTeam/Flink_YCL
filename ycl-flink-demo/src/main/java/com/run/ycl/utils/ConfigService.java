package com.run.ycl.utils;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class ConfigService {

    private static Map<String, String> mapStringCache;

    public static String getString(String key) {
        if (mapStringCache == null) {
            initMapCache();
        }
        String val = mapStringCache.get(key);
        if (StringUtils.isBlank(val)) {
            Jedis jedis = RedisUtils.getRedisClient();
            val = jedis.get(key);
            if (StringUtils.isNotBlank(val)) {
                mapStringCache.put(key, val);
            }
            if (null != jedis)
                jedis.close();
        }
        return val;
    }

    public static synchronized void initMapCache(){
        mapStringCache = new HashMap<String, String>();
    }
    @Test
    public void test() {
        System.out.println(ConfigService.getString("index_des_WA_MFORENSICS_020700"));;
    }
}
