package com.run.ycl.sideout;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class StatisticOutputFunc extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<StatisticPOJO> outputTag;
    private String module;
    public StatisticOutputFunc(OutputTag<StatisticPOJO> outputTag, String module){
        this.outputTag = outputTag;
        this.module = module;
    }
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        out.collect(value);
        StatisticPOJO statisticPOJO = new StatisticPOJO();
        statisticPOJO.setCount(1);
        statisticPOJO.setProtocol(value.getString("SOURCE_NAME"));
        statisticPOJO.setModule(module);
        ctx.output(outputTag,statisticPOJO);
    }

    public static class StatisticPOJO{
        String module;
        String protocol;
        long count;
        long startTime;
        long endTime;

        public String getModule() {
            return module;
        }

        public void setModule(String module) {
            this.module = module;
        }

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        @Override
        public String toString() {
            return "StatisticPOJO{" +
                    "module='" + module + '\'' +
                    ", protocol='" + protocol + '\'' +
                    ", count=" + count +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    '}';
        }
    }
}
