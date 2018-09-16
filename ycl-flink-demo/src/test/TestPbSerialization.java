import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.PbSequenceFileStreamOutput;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianghui on 18-8-24.
 */
public class TestPbSerialization {

    @Test
    public void testPbSequenceFileWriter() {

     /*   System.setProperty("HADOOP_HOME", "hadoop");
        System.setProperty("hadoop.home.dir", "");*/

        String[] args = new String[] {
                "--input-topic", "s911-003",
                //"--input-topic", "s911",
                "--output-topic", "source_144_WA_SOURCE_0001",
                "--bootstrap.servers", "192.168.244.100:9092,192.168.244.100:9092,192.168.244.100:9092",
                "--zoookeeper.connect", "192.168.244.100:2181",
                "--group.id", "9"
        };
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        FlinkKafkaConsumer010<String> flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-topic"),
                new SimpleStringSchema(),
                parameterTool.getProperties());
        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<String> input = env.addSource(flinkKafkaConsumer);

        input.print();

       List<String> list = new ArrayList<String>();
       list.add("WA_BASIC_0021");

       PbSequenceFileStreamOutput.pbSequenceFileOutput(list, input.flatMap(new FlatMapFunction<String, JSONObject>() {
           @Override
           public void flatMap(String value, Collector<JSONObject> out) throws Exception {
               int i = 50000;
               while(i-- > 0) {
                   JSONObject json = JSONObject.parseObject(value);
                   out.collect(json);
               }
           }
        }));

       /* SingleOutputStreamOperator<Tuple2<Text, Text>> dataMapRes = input.process(new ProcessFunction<String, Tuple2<Text, Text>>() {

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<Text, Text>> out) throws Exception {
                out.collect(new Tuple2<Text, Text>(new Text(value), new Text(value)));
            }
        });

        BucketingSink bucketingSink = new BucketingSink<String>("/user/sun/e6");
        bucketingSink.setWriter(new SequenceFileWriter<Text, Text>());
        org.apache.hadoop.conf.Configuration hdfsConfig = new org.apache.hadoop.conf.Configuration();
        hdfsConfig.set("fs.default.name", "hdfs://master:9000");
      //  hdfsConfig.setBoolean("mapreduce.output.fileoutputformat.compress", true);
      //  hdfsConfig.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);
        bucketingSink.setFSConfig(hdfsConfig);

        dataMapRes.addSink(bucketingSink);
        //dataMapRes.print();*/
        try {
            env.execute("Kafka 0.10 Example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
