package com.run.ycl.filter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.ConfigService;
import com.run.ycl.utils.DvUtils;
import com.run.ycl.utils.JsonUtils;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 数据清洗
 */
public class CommonDataValidateFilter implements FilterFunction<JSONObject> {

    private Map<String,JSONObject> map = new HashMap<>();

    @Override
    public boolean filter(JSONObject valueJson) throws Exception {
        boolean res = true;
        String protocol = valueJson.getString("SOURCE_NAME");
        JSONObject dataValidataJson = map.get(protocol);
        if(null ==dataValidataJson) {
            dataValidataJson = JSONObject.parseObject(ConfigService.getString("DataValidata_" + valueJson.getString("SOURCE_NAME")));
            if(null != dataValidataJson)
                map.put(protocol,dataValidataJson);
            else
                return true;
        }
        JSONArray normalizingArray= JsonUtils.getJSONArray(dataValidataJson, "NormalizedField");
        if(normalizingArray != null) {
            Iterator<Object> normalizedFieldIterator = normalizingArray.iterator();
            while (normalizedFieldIterator.hasNext()) {
                JSONObject normalizedFieldJson = (JSONObject) normalizedFieldIterator.next();
                res = res && validate(valueJson, normalizedFieldJson);
                //做多项校验时，是否需要做短路运算
                if (!res) {
                    break;
                }
            }
        }

        return res;
    }

    private boolean validate(JSONObject value, JSONObject normalizedField) {
        boolean validateFlag = true;
        JSONObject functionJson = normalizedField.getJSONObject("Expression");
        if (null != functionJson) {
            String functionName = functionJson.getString("Function");
            switch (functionName) {
                case "dv_must" :
                    validateFlag = DvUtils.dvMust(value,normalizedField);
                    break;
                default:
                    validateFlag = DvUtils.RegValidate(value,normalizedField);
                    break;
            }
            return validateFlag;
        }
        return true;
    }
}
