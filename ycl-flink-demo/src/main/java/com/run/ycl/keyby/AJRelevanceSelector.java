package com.run.ycl.keyby;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.StringTokenizer;


public class AJRelevanceSelector implements KeySelector<JSONObject,String> {
    private String elementStr;
    public AJRelevanceSelector() {
    }
    public AJRelevanceSelector(String elementStr) {
        this.elementStr = elementStr;
    }
    @Override
    public String getKey(JSONObject value) throws Exception {
        StringBuffer sb = new StringBuffer();
        if (elementStr != null && elementStr.length()>0) {
            StringTokenizer stringTokenizer = new StringTokenizer(elementStr, ",");
            while (stringTokenizer.hasMoreElements()){
                String element =(String) stringTokenizer.nextElement();
                String elementVal = value.getString(element);
                if (elementVal != null && elementVal.length()>0){
                    sb.append(elementVal);
                }
            }
        }
        return sb.toString();
    }
}
