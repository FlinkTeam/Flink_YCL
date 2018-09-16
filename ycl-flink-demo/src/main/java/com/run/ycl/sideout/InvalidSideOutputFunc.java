package com.run.ycl.sideout;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.filter.CommonDataValidateFilter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class InvalidSideOutputFunc extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    private CommonDataValidateFilter commonDataValidateFilter;
    public InvalidSideOutputFunc(OutputTag<JSONObject> outputTag, CommonDataValidateFilter commonDataValidateFilter){
        this.outputTag = outputTag;
        this.commonDataValidateFilter = commonDataValidateFilter;
    }
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

        if(commonDataValidateFilter.filter(value))
            out.collect(value);
        else
            ctx.output(outputTag,value);
    }
}
