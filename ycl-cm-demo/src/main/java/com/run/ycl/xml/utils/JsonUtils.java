package com.run.ycl.xml.utils;

import com.alibaba.fastjson.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JsonUtils {

    public static JSONArray getJSONArray(JSONObject jsonObject, String key) {
        JSONArray jsonArray = null;
        if (jsonObject != null) {
            try {
                if (jsonObject.has(key)) {
                    jsonArray = jsonObject.getJSONArray(key);
                }
            } catch (Exception e) {
                try {
                    jsonArray = new JSONArray("[" + jsonObject.getJSONObject(key).toString() + "]");
                } catch (Exception e1) {
                     e1.printStackTrace();
                }
            }
        }
        if (jsonArray == null) {
            jsonArray = new JSONArray();
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
}
