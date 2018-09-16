package com.run.ycl;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.filter.CommonDataValidateFilter;
import com.run.ycl.sideout.InvalidSideOutputFunc;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

/**
 * Created by jianghui on 18-8-29.
 */
public class DataValidate {
    public static SingleOutputStreamOperator<JSONObject> dataValidateHandle(SingleOutputStreamOperator<JSONObject> dataMapRes) {
        final OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("err"){};
        dataMapRes = dataMapRes.process(new InvalidSideOutputFunc(outputTag,new CommonDataValidateFilter()){});

        //校验失败的数据
        DataStream<JSONObject> invalidateDataRow = dataMapRes.getSideOutput(outputTag);

        return dataMapRes;
    }
}
