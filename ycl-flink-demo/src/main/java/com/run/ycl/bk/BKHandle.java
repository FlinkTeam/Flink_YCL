package com.run.ycl.bk;

import java.util.*;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.bk.BKConfigInit.Field;
import com.run.ycl.bk.BKConfigInit;
import com.run.ycl.utils.ClueUtils;
import com.run.ycl.utils.DuUtils;
import com.run.ycl.utils.RedisUtils;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class BKHandle {
    private static final int REDIS_DB_INDEX = 10;

    public static Map<String,Integer> getMatchResult(JSONObject jsonObject){
        Map<String,Integer> subMap = new HashMap<>();
        if(null != jsonObject && dataSetFilter(jsonObject)) {
            Jedis jedis = RedisUtils.getRedisClient();
            jedis.select(REDIS_DB_INDEX);
            Pipeline pipeline = jedis.pipelined();
            Map<String, Response<Set<String>>> responseMap = new HashMap<>();
            for (Field field : BKConfigInit.compareFieldMap.values()) {
                switch (field.getCompareMethod()){
                    case "single_string" :
                    case "num" :
                    case "ip" :
                        String fieldName = field.getFieldName();
                       // String normalValue = ClueUtils.getNormalValue(fieldName,jsonObject.getString(strReverse(fieldName)));
                        String normalValue = getNormalValue(fieldName,jsonObject);
                        String key = String.format("%s=%s",fieldName,normalValue);
                        responseMap.put(key,pipeline.smembers(key));
//                        Set<String> set = jedis.smembers(String.format("%s=%s",fieldName,jsonObject.getString(fieldName)));
//                        for(String item : set){
//                            if(!subMap.containsKey(item)){
//                                subMap.put(item,1);
//                            }else {
//                                subMap.put(item,subMap.get(item)+1);
//                            }
//                        }
                        break;
                    default:
                        break;
                }
            }
            pipeline.sync();
            for(Map.Entry<String,Response<Set<String>>> entry : responseMap.entrySet()) {
                for (String item : entry.getValue().get()) {
                    if (!subMap.containsKey(item)) {
                        subMap.put(item, 1);
                    } else {
                        subMap.put(item, subMap.get(item) + 1);
                    }
                }
            }
            jedis.close();
        }
        return subMap;
    }

    public static String getNormalValue(String fieldName, JSONObject jsonObject) {
        String normalValue = jsonObject.getString(fieldName);
        BKConfigInit.Field field = BKConfigInit.getCompareFieldMap().get(fieldName);
        if(field != null){
            String normal = field.getNormal();
            String normalType = field.getNormalMethod();
            if(normal != null && normal.equals("yes")){
                if(normalType!=null){
                    switch (normalType){
                        case "mobile":
                        case "idcard":
                        case "mac":
                        case "url":
                            normalValue = jsonObject.getString(strReverse(fieldName));
                            break;
                        case "general":
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        return  normalValue;
    }

    public static Map<String,Integer> dataSetFilter(Map<String,Integer> map, JSONObject jsonObject) {
        Map<String, Integer> mapTmp = new HashMap<>();
        if (null != jsonObject) {
            Map<String, Response<Boolean>> responseMap = new HashMap<>();
            String dataSet = jsonObject.getString("SOURCE_NAME");
            Jedis jedis = RedisUtils.getRedisClient();
            jedis.select(REDIS_DB_INDEX);
            Pipeline pipeline = jedis.pipelined();
            for (String item : map.keySet()) {
                responseMap.put(item, pipeline.getbit(dataSet, Long.valueOf(item)));
//                if(jedis.getbit(dataSet,Long.valueOf(item))){
//                    mapTmp.put(item,map.get(item));
//                }
            }
            pipeline.sync();
            for (Map.Entry<String, Response<Boolean>> entry : responseMap.entrySet()) {
                if (entry.getValue().get())
                    mapTmp.put(entry.getKey(), map.get(entry.getKey()));
            }
            jedis.close();
        }
        return mapTmp;
    }

    public static Map<String,List<Map<String,String>>> getMatchClueInfo(Map<String,Integer> map){

        Map<String,List<Map<String,String>>> map2 = new HashMap<>();
        if(!map.isEmpty()) {
            Jedis jedis = RedisUtils.getRedisClient();
            jedis.select(REDIS_DB_INDEX);
            Pipeline pipeline = jedis.pipelined();
            Map<String, Response<String>> responseMap = new HashMap<>();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String key = entry.getKey() + "@" + entry.getValue();
                responseMap.put(key, pipeline.get(key));
            }
            pipeline.sync();
            Set<String> set = new HashSet<>();
            for (Map.Entry<String, Response<String>> entry : responseMap.entrySet()) {
                String clueid = entry.getValue().get();
                if(!clueid.isEmpty())
                    set.add(clueid);
            }
            Map<String, Response<Map<String,String>>> responseMap2 = new HashMap<>();
            for(String clueID : set) {
                responseMap2.put(clueID, pipeline.hgetAll(clueID));
            }
            pipeline.sync();
            for (Map.Entry<String, Response<Map<String,String>>> entry : responseMap2.entrySet()) {
                String systemId = entry.getKey().substring(0, 3);
                if (!map2.containsKey(systemId)) {
                    List<Map<String, String>> list2 = new ArrayList<>();
                    list2.add(entry.getValue().get());
                    map2.put(systemId, list2);
                } else {
                    map2.get(systemId).add(entry.getValue().get());
                }
            }
            jedis.close();
        }
        return map2;
    }


    public static boolean dataSetFilter(JSONObject jsonObject){
        if(null != jsonObject)
            return BKConfigInit.compareDataSet.contains(jsonObject.getString("SOURCE_NAME"));
        return false;
    }

    public static String strReverse(String string){
        if(null == string || string.length() == 0)
            return string;
        char [] array = string.toCharArray();
        int length = string.length() -1;
        for(int i = 0; i< length ;i++,length--){
            array[i] ^= array[length];
            array[length] ^=array[i];
            array[i] ^= array[length];
        }
        return new String(array);
    }

}
