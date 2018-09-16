package com.run.ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class JsonUtils {

    public static JSONArray getJSONArray(JSONObject jsonObject, String key) {
        JSONArray jsonArray = null;
        if (jsonObject != null) {
            try {
                jsonArray = jsonObject.getJSONArray(key);
            } catch (ClassCastException e) {
                jsonArray = JSONArray.parseArray("[" + jsonObject.getJSONObject(key).toJSONString() + "]");
            }
        }
        return jsonArray;
    }

    public static Map<String, String> convertJsonArrayToMap(JSONArray jsonArray, String mapKey, String mapValue) {
        Map<String, String> map = new HashMap<String, String>();
        if (jsonArray != null) {
            Iterator<Object> iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                map.put(jsonObject.getString(mapKey), jsonObject.getString(mapValue));
            }
        }
        return map;
    }

    public static JSONObject convertWaToRwa(JSONObject waJson) {
        JSONObject rwaJson = new JSONObject();
        Set<String> keySet = waJson.keySet();
        for (String key: keySet) {
            Object value = waJson.get(key);
            rwaJson.put("R" + key, value);
        }
        return rwaJson;
    }
}
