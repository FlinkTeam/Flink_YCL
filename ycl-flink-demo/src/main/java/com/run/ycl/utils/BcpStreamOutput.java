package com.run.ycl.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.hadoop.conf.Configuration;

public class BcpStreamOutput {
    public BcpStreamOutput(DataStream<String> stream, String path){
        //BK数据保存到HDFS
        String pathTimeFormat = "yyyy-MM-dd--HH";
        //long batchSize = 1024 * 1024 * 400;
        long batchSize =  1024*1024*80;
        BucketingSink<String> bkData2hdfs = new BucketingSink<>(path);
        bkData2hdfs.setBucketer(new DateTimeBucketer<String>(pathTimeFormat));
        bkData2hdfs.setBatchSize(batchSize);
        Configuration hdfsConfig = new Configuration();
        hdfsConfig.set("fs.defaultFS", "hdfs://192.168.251.73:8020/");
        //hdfsConfig.set("fs.defaultFS", "hdfs://192.168.251.73:8020/");
        bkData2hdfs.setFSConfig(hdfsConfig);
        //hdfsConfig.set("hadoop.tmp.dir", "/home/hadoop/data/tmp");
        stream.addSink(bkData2hdfs).name(path).setParallelism(1);
    }
}
