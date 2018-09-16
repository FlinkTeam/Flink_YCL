package com.run.ycl.sideout;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.bk.BKHandle;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BKSideOutputFunc extends ProcessFunction<JSONObject,JSONObject> {

    private Map<String, OutputTag<JSONObject>> outputTagMap;

    public BKSideOutputFunc(Map<String, OutputTag<JSONObject>> tagMap){
        this.outputTagMap = tagMap;
    }
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {


        boolean internalFlag = true;
        boolean noDiscardFlag = true;
        internalFlag = BKHandle.dataSetFilter(value);
        if(null == value)
            return;
        if(!internalFlag){
            out.collect(value);
        }else {
            Map<String, List<Map<String,String>>> matchMap =
                    BKHandle.getMatchClueInfo(
                            BKHandle.dataSetFilter(
                                    BKHandle.getMatchResult(value),value));

            if(!matchMap.isEmpty()){
                for(Map.Entry<String,List<Map<String,String>>> entry : matchMap.entrySet()){
                    List<Map<String,String>> list = entry.getValue();
                    noDiscardFlag = backFilling(list,value);
                    //String key = String.format("%s/%s",list.get(0).get("clueid").substring(0,3),value.getString("SOURCE_NAME"));
                    String key = String.format("%s",list.get(0).get("clueid").substring(0,3));
                    if(outputTagMap.keySet().contains(key))
                        ctx.output(outputTagMap.get(key), value);
                }
            }
            if(noDiscardFlag)
                out.collect(value);
        }
    }

    public boolean backFilling(List<Map<String,String>> clueMap, JSONObject jsonObject){
        boolean noDiscard = true;
        for(Map<String,String> map : clueMap){
            String [] actions = map.get("actions").split("\t");
            for(String action : actions){
                String [] opration = action.split(",");
                switch (opration[0]){
                    case "fill":
                        switch (opration[3]){
                            case "add":
                                fillFields(jsonObject, opration);
                                break;
                            default:
                                break;
                        }
                        break;
                    case "discard":
                        fillFields(jsonObject, opration);
                        noDiscard = false;
                        break;
                    default:
                        break;
                }
            }
        }
        return noDiscard;
    }

    private void fillFields(JSONObject jsonObject, String[] opration) {
        String strTmp = jsonObject.getString(opration[1]);
        if (strTmp != null) {
            if (!strTmp.isEmpty())
                strTmp = strTmp + ":" + opration[2];
            else
                strTmp = opration[2];
        } else
            strTmp = opration[2];
        jsonObject.put(opration[1], strTmp);
    }
}
