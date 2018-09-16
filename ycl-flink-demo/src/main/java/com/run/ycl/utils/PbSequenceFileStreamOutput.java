package com.run.ycl.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import java.util.ArrayList;
import java.util.List;

public class PbSequenceFileStreamOutput {

    private static String pathTimeFormat = "yyyy-MM-dd--HH";
    private static final String HDFS_PRE = "/user/jyt1/";

    private static long batchSize =  1024 * 1024 * 10; //10M

    private static String defaultFs = "hdfs://192.168.251.73:8020";

    //目前只输出sourceList指定的协议
    public static SingleOutputStreamOperator<JSONObject> pbSequenceFileOutput(List<String> sourceList, SingleOutputStreamOperator<JSONObject> stream) {

        //将数据按照协议进行划分流
        SplitStream<JSONObject> splitStream = stream.split(new OutputSelector<JSONObject>() {
            public Iterable<String> select(JSONObject valueJson) {
                List<String> output = new ArrayList<String>();
                String protocol = valueJson.getString("SOURCE_NAME");
                if (StringUtils.isNotBlank(protocol) && !output.contains(protocol)) {
                    output.add(protocol);
                }
                return output;
            }
        });

        if (sourceList != null && sourceList.size() > 0) {
            for (String source: sourceList) {
                String path = HDFS_PRE + source;
                outputStreamWithPbSequenceFile(splitStream.select(source), path);
            }
        }

        return stream;
    }

    //最终数据以PB+SequenceFile的形式保存到HDFS
    private static void outputStreamWithPbSequenceFile(DataStream<JSONObject> stream, String path){

        SingleOutputStreamOperator<Tuple2<BytesWritable, BytesWritable>> output = stream.process(new ProcessFunction<JSONObject, Tuple2<BytesWritable, BytesWritable>>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<Tuple2<BytesWritable, BytesWritable>> out) throws Exception {
                String protocolName = value.getString("SOURCE_NAME");
                protocolName = "R" + protocolName;
                JSONObject rwaJso = JsonUtils.convertWaToRwa(value);
                out.collect(new Tuple2<>(new BytesWritable(("S003" + "," +protocolName).getBytes()), new BytesWritable(ProtoBufUtil.jsonToProtoBtyeArray(protocolName, rwaJso.toString()))));
            }
        });

        BucketingSink<Tuple2<BytesWritable, BytesWritable>> sequenceFlie2HDFSSink = new BucketingSink<>(path);
        sequenceFlie2HDFSSink.setBucketer(new DateTimeBucketer<>(pathTimeFormat));
        //sequenceFlie2HDFSSink.setBatchSize(batchSize);

        SequenceFileWriter sequenceFileWriter = new SequenceFileWriter<BytesWritable, BytesWritable>("org.apache.hadoop.io.compress.SnappyCodec", SequenceFile.CompressionType.RECORD);
        sequenceFlie2HDFSSink.setWriter(sequenceFileWriter);

        Configuration hdfsConfig = new Configuration();
        hdfsConfig.set("fs.defaultFS", defaultFs);
       // hdfsConfig.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);

        sequenceFlie2HDFSSink.setFSConfig(hdfsConfig);

        output.addSink(sequenceFlie2HDFSSink).setParallelism(1);
    }
}
