package com.run.ycl;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.bk.BKConfigInit;
import com.run.ycl.bk.BKStreamOutput;
import com.run.ycl.map.CreateBkBcpMap;
import com.run.ycl.sideout.BKSideOutputFunc;
import com.run.ycl.utils.RowStreamOutput;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * Created by jianghui on 18-8-29.
 */
public class BK {
    public static SingleOutputStreamOperator<JSONObject> bkHandle(SingleOutputStreamOperator<JSONObject> dataMapRes) {
        Map<String,OutputTag<JSONObject>> bkOutputTag = BKConfigInit.bkOutputTag;
        dataMapRes = dataMapRes.process(new BKSideOutputFunc(bkOutputTag){});
        for (Map.Entry<String,OutputTag<JSONObject>> entry : bkOutputTag.entrySet()){
            //String protocolName = entry.getKey().substring(entry.getKey().lastIndexOf('/')+1);
            //DataStream<String> bkOutputStream = dataMapRes.getSideOutput(entry.getValue()).map(new CreateBkBcpMap(protocolName));
            DataStream<JSONObject> bkOutputStream = dataMapRes.getSideOutput(entry.getValue());
            new BKStreamOutput(bkOutputStream, String.format("/%s/%s","BKOutput",entry.getKey()));
            //bkOutputStream.print();//just for test
        }
        return dataMapRes;
    }
}
