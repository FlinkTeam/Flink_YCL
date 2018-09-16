package com.run.ycl.utils;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {

    public static JedisPoolConfig poolConfig;
    public static JedisPool pool;

    public static void init() {
        //设置连接池属性
        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(8);
        poolConfig.setMaxTotal(18);

        pool = new JedisPool(poolConfig, "192.168.251.73");
    }

    public static Jedis getRedisClient() {
        if (pool == null) {
            init();
        }
        return pool.getResource();
    }

    public void closed(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Test
    public void get() {
        System.out.println(RedisUtils.getRedisClient().get("a"));
    }
}
