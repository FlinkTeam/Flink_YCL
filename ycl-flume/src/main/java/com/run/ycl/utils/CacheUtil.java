package com.run.ycl.utils;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.run.ycl.parser.file.IndexDesBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class CacheUtil {

    private static final Logger logger = LoggerFactory.getLogger(CacheUtil.class);

    public static Cache<String, String> commonStringCache = Caffeine.newBuilder()
            .expireAfterWrite(15, TimeUnit.MINUTES)
            .maximumSize(10000000)
            .build();
    public static Cache<String, String> commonStringExistCache = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .maximumSize(10000000)
            .build();

    public static Cache<String, IndexDesBean> indexCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(1000000)
            .build();
    public static Cache<String, String> xmlFileNameCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(10000000)
            .build();

    //针对非标准化的bcp文件，存放具体的解析映射规则
    public static Cache<String, IndexDesBean> nonstandardBcpIndexCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(100000)
            .build();
    //针对非标准化的bcp文件，是否存在解析规则，如存在，则进一步在nonstandardBcpIndexCache中查找
    public static Cache<String, String> nonstandardBcpIndexExistCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(100000)
            .build();

    public static String getIfPresentFromCommonString(String key) {
        key = key.toUpperCase();

        String isExist = commonStringExistCache.getIfPresent(key);
        if (logger.isDebugEnabled()) {
            logger.debug("cache isExist:{}==key:{}", new String[] {isExist, key});
        }
        if (!StringUtils.isNotBlank(isExist)) {
            //如果没有获取过解析规则，更新缓存
            updateCommonStringCache(key);
        }
        return commonStringCache.getIfPresent(key);
    }

    private static void updateCommonStringCache(String key) {
        String cacheKey = key.toUpperCase();

        Jedis jedis = null;
        try {
            jedis = RedisUtils.getRedisClient();

            String cacheVal = jedis.get(cacheKey);
            commonStringExistCache.put(cacheKey, "true");

            if (StringUtils.isNotBlank(cacheVal)) {
                commonStringCache.put(cacheKey, cacheVal);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
    public static IndexDesBean getIfPresentFromNonstandardBcpIndex(String dataSource, String sourceName) {
        dataSource = dataSource.toUpperCase();
        sourceName = sourceName.toUpperCase();

        if (logger.isDebugEnabled()) {
            logger.debug("get cache bean==dataSource:{}, sourceName:{}", dataSource, sourceName);
        }
        String key = dataSource + "_index_des_" + sourceName;
        String isExist = nonstandardBcpIndexExistCache.getIfPresent(key);
        if (logger.isDebugEnabled()) {
            logger.debug("cache isExist:{}==dataSource:{}, sourceName:{}", new String[] {isExist, dataSource, sourceName});
        }

        if (!StringUtils.isNotBlank(isExist)) {
            //如果没有获取过解析规则，更新缓存
            updateNonstandardBcpIndexCache(dataSource, sourceName);
        }
        return nonstandardBcpIndexCache.getIfPresent(key);
    }

    //redis 中存放的key SOURCE_999_SF_index_des_DELIVERY_SF
    public static void updateNonstandardBcpIndexCache(String dataSource, String sourceName) {
        dataSource = dataSource.toUpperCase();
        sourceName = sourceName.toUpperCase();

        Jedis jedis = null;
        try {
            jedis = RedisUtils.getRedisClient();
            String cacheKey = dataSource + "_index_des_" + sourceName;
            String indexStr = jedis.get(cacheKey);
            nonstandardBcpIndexExistCache.put(cacheKey, "true");

            if (StringUtils.isNotBlank(indexStr)) {
                //将json解析成IndexDesBean对象
                IndexDesBean indexDes = new IndexDesBean();
                JSONObject indexJson = JSONObject.parseObject(indexStr);

                JSONArray itemJsonArray =JsonUtils.getJSONArray(indexJson, "ITEM");
                Iterator<Object> itemIterator = itemJsonArray.iterator();
                while (itemIterator.hasNext()) {
                    JSONObject itemJson = (JSONObject) itemIterator.next();
                    String itemVal = itemJson.getString("val");
                    String itemKey = itemJson.getString("key");
                    if ("A010004".equalsIgnoreCase(itemKey)) {
                        indexDes.setdDataset(jedis.get(dataSource + "_in2out_" + itemVal.toUpperCase()));
                        indexDes.setsDataset(itemVal.toUpperCase());
                    } else if ("I010038".equalsIgnoreCase(itemKey)) {
                        indexDes.setDataBeginLineNum(Integer.valueOf(itemVal));
                    }
                }

                JSONArray dataSetJsonArray =JsonUtils.getJSONArray(indexJson, "DATASET");
                Iterator<Object> dataSetIterator = dataSetJsonArray.iterator();
                while (dataSetIterator.hasNext()) {
                    JSONObject dataSetJson = (JSONObject) dataSetIterator.next();
                    String dataSetName = dataSetJson.getString("name");
                    if ("WA_COMMON_010014".equalsIgnoreCase(dataSetName)) {
                        //BCP文件信息
                        JSONArray datasetItemJsonArray = JsonUtils.getJSONArray(dataSetJson.getJSONObject("DATA"), "ITEM");
                        Iterator<Object> dataSetItemIterator = datasetItemJsonArray.iterator();
                        while (dataSetItemIterator.hasNext()) {
                            JSONObject dataSetItemJson = (JSONObject) dataSetItemIterator.next();
                            String datasetItemKey = dataSetItemJson.getString("key");
                            String datasetItemVal = dataSetItemJson.getString("val");
                            if ("H010020".equalsIgnoreCase(datasetItemKey)) {
                                indexDes.setFileName(datasetItemVal);
                            }
                        }
                    } else if ("WA_COMMON_010015".equalsIgnoreCase(dataSetName)) {
                        //BCP文件数据结构
                        JSONArray datasetItemJsonArray = JsonUtils.getJSONArray(dataSetJson.getJSONObject("DATA"), "ITEM");
                        Integer size = datasetItemJsonArray.size();
                        if (size > 0) {
                            String[] items = new String[size];
                            int i = 0;
                            Iterator<Object> dataSetItemIterator = datasetItemJsonArray.iterator();
                            while (dataSetItemIterator.hasNext()) {
                                JSONObject dataSetItemJson = (JSONObject) dataSetItemIterator.next();
                                String key = dataSetItemJson.getString("key");
                                items[i++] = key;
                            }
                            indexDes.setItems(items);
                        }
                    }
                }
                nonstandardBcpIndexCache.put(cacheKey, indexDes);
                if (logger.isDebugEnabled()) {
                    logger.debug("update cache:==cacheKey:{}, indexDes:{}", new String[] {cacheKey, indexDes.toString()});
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("end print :{}", new String[] {nonstandardBcpIndexCache.getIfPresent(cacheKey).toString()});
                }
                System.out.println("zhege:"+cacheKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            RedisUtils.closed(jedis);
        }
    }
}
