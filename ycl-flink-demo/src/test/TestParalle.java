import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TestParalle {

    public static void main(String[] args) {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 9) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>" +
                    "--dbhost 192.168.251.73 --dbname testdb --username root --password root");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.readFile()
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
        //flinkKafkaConsumer.setStartFromGroupOffsets();

        DataStream<String> input = env.addSource(flinkKafkaConsumer);

        //SequenceFileWriter writer;
        input.print();

        SingleOutputStreamOperator<Tuple2<Text, BytesWritable>> output = input.process(new ProcessFunction<String, Tuple2<Text, BytesWritable>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<Text, BytesWritable>> out) throws Exception {
                out.collect(new Tuple2<>(new Text(value), new BytesWritable(value.getBytes())));
            }
        });
        String pathTimeFormat = "yyyy-MM-dd--HH-mm";
        long batchSize = 1024 * 1024 * 400;
        BucketingSink<Tuple2<Text, BytesWritable>> sequenceFlie2HDFSSink = new BucketingSink<>("tt");
        sequenceFlie2HDFSSink.setBucketer(new DateTimeBucketer<>(pathTimeFormat));
        sequenceFlie2HDFSSink.setBatchSize(batchSize);
        sequenceFlie2HDFSSink.setWriter(new SequenceFileWriter<Text, BytesWritable>());

        Configuration hdfsConfig = new Configuration();
        hdfsConfig.set("fs.defaultFS", "hdfs://192.168.251.73:8020");
        sequenceFlie2HDFSSink.setFSConfig(hdfsConfig);

        output.addSink(sequenceFlie2HDFSSink);
       // SequenceFileWriter sequenceFileWriter = new SequenceFileWriter<Text, ByteWritable>();
       // sequenceFileWriter.write(new );
/*
        SingleOutputStreamOperator<JSONObject> dataMapRes =  input.map(new CommonDataMappingMapper());

        SplitStream<JSONObject> soureSplitStream = dataMapRes.split(new PreRelatedSplitter());
        DataStream<JSONObject> s20700DataStream = soureSplitStream.select("WA_MFORENSICS_020700");

        s20700DataStream.print();*/

        /*
        //关键人物统计
        if (true) {
            //基于提取出的手机号之间的关系，统计关键人物
            soureSplitStream.select("WA_MFORENSICS_020700")
                    //可进一步优化只取主动呼叫的数据
                    .keyBy(new KeySelector<JSONObject, Tuple2<String, Integer>>() {
                        public Tuple2<String, Integer> getKey(JSONObject value) throws Exception {
                            Tuple2<String, Integer> essentialPerson = new Tuple2<String, Integer>();
                            essentialPerson.f0 = value.getString("");
                            essentialPerson.f1 = 1;
                            return essentialPerson;
                        }
                    })
                    .sum(1);
            // .process(new ProcessFuncti);
        }
*/

        try {
            env.execute("Kafka 0.10 Example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
