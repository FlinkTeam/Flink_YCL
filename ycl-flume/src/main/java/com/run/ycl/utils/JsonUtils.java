package com.run.ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class JsonUtils {

    public static JSONArray getJSONArray(JSONObject jsonObject, String key) {
        JSONArray jsonArray = null;
        if (jsonObject != null) {
            try {
                jsonArray = jsonObject.getJSONArray(key);
            } catch (Exception e) {
                jsonArray = JSONArray.parseArray("[" + jsonObject.getJSONObject(key).toString() + "]");
            }
        }
        return jsonArray;
    }

}
