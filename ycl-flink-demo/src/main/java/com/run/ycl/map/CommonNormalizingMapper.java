package com.run.ycl.map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.ConfigService;
import com.run.ycl.utils.DuUtils;
import com.run.ycl.utils.JsonUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 归一化处理
 * Normalizing.xml
 */
public class CommonNormalizingMapper implements MapFunction<JSONObject, JSONObject> {


    public CommonNormalizingMapper() {
    }

    @Override
    public JSONObject map(JSONObject valueJson) throws Exception {

        String sourceName = valueJson.getString("SOURCE_NAME");
        String normalizing_str = ConfigService.getString("Normalizing_" + sourceName);
        if (normalizing_str != null){
            JSONObject normalizingJson = JSONObject.parseObject(normalizing_str);
            JSONArray normalizedFieldArray= JsonUtils.getJSONArray(normalizingJson, "NormalizedField");
            normalizedFieldArray.forEach(normalizedField -> {
                JSONObject normalizedFieldJson = (JSONObject) normalizedField;
                handle(valueJson, normalizedFieldJson);
            });
        }
        return valueJson;
    }

    //DU_NORMAL, DU_MAC, DU_MOBILE, DU_URL, DU_CARD, DU_TRIM_DELIM, DU_BSID,
    private void handle(JSONObject value, JSONObject normalizedField) {
        JSONObject functionJson = normalizedField.getJSONObject("Expression");
        if (null != functionJson) {
            String functionName = functionJson.getString("Function");
            DuUtils.all_normalizing(value, normalizedField, functionName);
        }
    }
}
