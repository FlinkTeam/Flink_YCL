package com.run.ycl.map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.ConfigService;
import com.run.ycl.utils.DmUtils;
import com.run.ycl.utils.ExtractUtils;
import com.run.ycl.utils.JsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class CommonFlatMapingMapper implements FlatMapFunction<JSONObject, JSONObject> {

    @Override
    public void flatMap(JSONObject value, Collector out) throws Exception {
        out.collect(value);

        String sourceName = value.getString("SOURCE_NAME");
        //对象提取
        String template = ConfigService.getString("Extractor_" + sourceName);
        if (StringUtils.isNotBlank(template)) {
            boolean valBool = true;
            JSONObject extConfJson = JSONObject.parseObject(template);
            valBool = this.validateExpression(value, extConfJson.getJSONObject("Conditions"));
            if (valBool) {
                JSONObject extJson = ExtractUtils.extract(value, extConfJson);
                out.collect(extJson);
            }
        }
    }

    /**
     * <Conditions Rel="AND">
     *     <Conditions Rel="NOT">
     *         <Conditions Rel="NULL">
     *             <Condition Fmt="" Key="B020005" Opr="" Value=""/>
     *         </Conditions>
     *     </Conditions>
     *     <Conditions Rel="NOT">
     *         <Conditions Rel="NULL">
     *             <Condition Fmt="" Key="B070003" Opr="" Value=""/>
     *         </Conditions>
     *     </Conditions>
     * </Conditions>
     **/
    private boolean validateExpression(JSONObject valueJson, JSONObject conditionsJson) {
        boolean expBool = true;

        if (conditionsJson != null) {
            String expType = conditionsJson.getString("Rel");
            JSONArray conditionArray = JsonUtils.getJSONArray(conditionsJson, "Conditions");
            if (conditionArray != null) {
                Iterator<Object> conditionIterator = conditionArray.iterator();
                while (conditionIterator.hasNext()) {
                    JSONObject condition = (JSONObject) conditionIterator.next();
                    if ("OR".equals(expType)) {
                        expBool = false;
                        expBool = expBool || validateCondition(valueJson, condition);
                        if (expBool == true) {
                            //短路
                            break;
                        }
                    } else if ("AND".equals(expType)) {
                        expBool = true;
                        expBool = expBool && validateCondition(valueJson, condition);
                        if (expBool == false) {
                            //短路
                            break;
                        }
                    }
                }
            }
        }
        return expBool;
    }

    /**
     * <Conditions Rel="NOT">
     *     <Conditions Rel="NULL">
     *         <Condition Fmt="" Key="B020005" Opr="" Value=""/>
     *     </Conditions>
     * </Conditions>
     *
     * <Conditions Rel="NOT">
     *     <Conditions Rel="NULL">
     *         <Condition Fmt="" Key="B070003" Opr="" Value=""/>
     *     </Conditions>
     * </Conditions>
     *
     * @param valueJson
     * @param condition
     * @return
     */
    private static boolean validateCondition(JSONObject valueJson, JSONObject condition) {
        boolean expBool = true;
        if (condition != null) {
            String rel1 = condition.getString("Rel");
            String rel2 = condition.getJSONObject("Conditions").getString("Rel");
            String validateKey = condition.getJSONObject("Conditions").getJSONObject("Condition").getString("Key");
            String validateValue = valueJson.getString(validateKey);
            if ("NOT".equalsIgnoreCase(rel1) && "NULL".equalsIgnoreCase(rel2)) {
                //处理 not null 情况校验
                if (StringUtils.isNotBlank(validateValue)) {
                    expBool = true;
                } else {
                    expBool = false;
                }
            }
        }
        return expBool;
    }
}
