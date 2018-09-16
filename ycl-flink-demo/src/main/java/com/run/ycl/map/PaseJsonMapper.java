package com.run.ycl.map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.ConfigService;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Iterator;

public class PaseJsonMapper implements MapFunction<JSONObject, String> {

    public PaseJsonMapper(){

    }
    @Override
    public String map(JSONObject valueJson) throws Exception {
        StringBuffer sb = new StringBuffer();
        if (valueJson != null){
            String dataId = "dataid";
            String sourceName = valueJson.getString("SOURCE_NAME");
            //现将sourcename当做dataid
            sb.append(sourceName + "\t");
            String index_str = ConfigService.getString("index_des_" + sourceName);
            if (index_str != null){
                JSONObject templateJson = JSONObject.parseObject(index_str);
                JSONArray dataSetArray = templateJson.getJSONArray("DATASET");
                Iterator<Object> dataSetIterator = dataSetArray.iterator();
                while (dataSetIterator.hasNext()) {
                    JSONObject dataSetJson = (JSONObject) dataSetIterator.next();
                    if (dataSetJson.getString("name").equals("WA_COMMON_010015")) {
                        JSONObject contentJson = dataSetJson.getJSONObject("DATA");
                        Iterator<Object> itemItertor = contentJson.getJSONArray("ITEM").iterator();
                        int i = 0;
                        while (itemItertor.hasNext()) {
                            JSONObject itemJson = (JSONObject) itemItertor.next();
                            String code = itemJson.getString("key");
                            String value = valueJson.getString(code);
                            sb.append(value);
                            sb.append("\t");
                        }
                    }
                }
            }
        }
        return sb.substring(0, sb.length()-1);
    }
}
