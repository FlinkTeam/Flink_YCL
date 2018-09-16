package com.run.ycl.utils;

import com.run.ycl.bk.ZipfileWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.hadoop.conf.Configuration;

public class RowStreamOutput {

    private DataStream<String> stream;
    private String path;
    public RowStreamOutput(DataStream<String> stream, String path){
        this.stream = stream;
        this.path =path;
        setConfig();
    }
    private void setConfig(){
        //BK数据保存到HDFS
        String pathTimeFormat = "yyyy-MM-dd--HH";
        long batchSize =  1024*1024*80;
        String prefix = String.format("%s-%s-%s-%s-%s",137,"705420347","010000","010000",System.currentTimeMillis()/1000);
        BucketingSink<String> bkData2hdfs = new BucketingSink<>(path);
        bkData2hdfs.setBucketer(new DateTimeBucketer<String>(pathTimeFormat));
        bkData2hdfs.setBatchSize(batchSize);
        String protocol = path.substring(path.lastIndexOf('/')+1);
        bkData2hdfs.setPartPrefix(prefix);
        bkData2hdfs.setPartSuffix(".zip");
        bkData2hdfs.setWriter(new ZipfileWriter<String>(protocol));
        Configuration hdfsConfig = new Configuration();
        hdfsConfig.set("fs.defaultFS", "hdfs://192.168.251.73:8020/");
        bkData2hdfs.setFSConfig(hdfsConfig);
        stream.addSink(bkData2hdfs).name(path);
        //stream.addSink(bkData2hdfs).name(path).setParallelism(1);
    }

}
