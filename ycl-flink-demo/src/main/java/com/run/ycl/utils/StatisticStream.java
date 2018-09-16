package com.run.ycl.utils;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.sideout.StatisticOutputFunc;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class StatisticStream {

    public static SingleOutputStreamOperator<StatisticOutputFunc.StatisticPOJO> getStatistcStream(SingleOutputStreamOperator<JSONObject> dataMapRes, String protocol, String module) {

        OutputTag<StatisticOutputFunc.StatisticPOJO> statisticPOJOOutputTag = new OutputTag<StatisticOutputFunc.StatisticPOJO>("statistic_"+protocol+module){};
        DataStream<StatisticOutputFunc.StatisticPOJO> statisticPOJODataStream = dataMapRes
                .process(new StatisticOutputFunc(statisticPOJOOutputTag,module))
                .getSideOutput(statisticPOJOOutputTag);

         SingleOutputStreamOperator<StatisticOutputFunc.StatisticPOJO> singleOutputStreamOperator = statisticPOJODataStream
                .keyBy(new KeySelector<StatisticOutputFunc.StatisticPOJO, String>() {
                    @Override
                    public String getKey(StatisticOutputFunc.StatisticPOJO value) throws Exception {
                        return value.getProtocol();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<StatisticOutputFunc.StatisticPOJO, StatisticOutputFunc.StatisticPOJO, String, TimeWindow>() {
                    @Override
                    public void apply(String o, TimeWindow window, Iterable<StatisticOutputFunc.StatisticPOJO> input, Collector<StatisticOutputFunc.StatisticPOJO> out) throws Exception {
                        long count = 0;
                        for (StatisticOutputFunc.StatisticPOJO statisticPOJO: input) {
                            count += statisticPOJO.getCount();
                        }
                        StatisticOutputFunc.StatisticPOJO tmp=  new StatisticOutputFunc.StatisticPOJO();
                        tmp.setCount(count);
                        tmp.setStartTime(window.getStart());
                        tmp.setEndTime(window.getEnd());
                        tmp.setModule(module);
                        tmp.setProtocol(o);
                        out.collect(tmp);
                    }
                });
         return singleOutputStreamOperator;
    }
}
