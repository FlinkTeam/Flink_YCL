package com.run.ycl.utils;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Iterator;

public class ExtractUtils {

    /**
     * <Field Element="H040002">
     *     <Expression Function="equal">
     *         <Param Name="element" Value="H040002"/>
     *     </Expression>
     * </Field>
     *
     * <Field Element="ZZZH040002">
     *     <Expression Function="const">
     *         <Param Name="const" Value="01"/>
     *     </Expression>
     * </Field>
     *
     *
     * @param valueJson
     * @param extField
     * @return
     */
    public static JSONObject extract(JSONObject valueJson, JSONObject extField) {
        JSONObject extJson = new JSONObject();
        extJson.put("SOURCE_NAME", extField.getString("DDataSet"));

        JSONArray fieldArray = JsonUtils.getJSONArray(extField.getJSONObject("Extractor"), "Field");
        Iterator<Object> fieldIterator = fieldArray.iterator();
        while (fieldIterator.hasNext()) {
            JSONObject fieldJson = (JSONObject) fieldIterator.next();
            String targetKey = fieldJson.getString("Element");
            String functionName = fieldJson.getJSONObject("Expression").getString("Function");
            String paramValue = fieldJson.getJSONObject("Expression").getJSONObject("Param").getString("Value");
            if ("const".equalsIgnoreCase(functionName)) {
                //常量赋值
                extJson.put(targetKey, paramValue);
            } else if ("equal".equalsIgnoreCase(functionName)) {
                //字段拷贝
                extJson.put(targetKey, valueJson.getString(paramValue));
            }
        }
        return extJson;
    }
}
