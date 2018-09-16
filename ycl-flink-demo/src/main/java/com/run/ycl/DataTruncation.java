package com.run.ycl;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.sideout.TruncationSideOutput;
import com.run.ycl.utils.ConfigService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTruncation {
    private static final Logger logger = LoggerFactory.getLogger(DataTruncation.class);

    public static SingleOutputStreamOperator<JSONObject>  dataTruncationHandle(SingleOutputStreamOperator<JSONObject> dataMapRes) {
        //进行数据截断
        String dataset_fieldset = ConfigService.getString("DATASET_FIELDSET");

        JSONObject protocolJsons = (JSONObject)JSONObject.parse(dataset_fieldset);

        final OutputTag<JSONObject> truncaOutputTag = new OutputTag<JSONObject>("dataTruncation"){};
        //进行数据截断
        dataMapRes = dataMapRes.process(new TruncationSideOutput(truncaOutputTag, protocolJsons));

        //得到侧输出流（数据超长字段）
        DataStream<JSONObject> longValStream = dataMapRes.getSideOutput(truncaOutputTag);

        return dataMapRes;
    }
}
