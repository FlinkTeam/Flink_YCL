package com.run.ycl.map;


        import com.alibaba.fastjson.JSONObject;
        import com.run.ycl.sideout.StatisticOutputFunc;
        import org.apache.flink.api.common.functions.MapFunction;
        import org.apache.flink.types.Row;

        import java.util.*;

public class CreateStatisticMap implements MapFunction<StatisticOutputFunc.StatisticPOJO, Row> {
    @Override
    public Row map(StatisticOutputFunc.StatisticPOJO value) throws Exception {
        Row row = Row.of(value.getModule(),value.getProtocol(),value.getCount(),value.getStartTime(),value.getEndTime());
        return row;
    }
}
