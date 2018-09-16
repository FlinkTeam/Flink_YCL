package com.run.ycl.sideout;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OutputTagFunc extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    public OutputTagFunc(OutputTag<JSONObject> outputTag){
        this.outputTag = outputTag;
    }
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        out.collect(value);
        ctx.output(outputTag,value);
    }
}
