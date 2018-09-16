package com.run.ycl;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.map.CommonDataMappingMapper;
import com.run.ycl.map.CommonFlatMapingMapper;
import com.run.ycl.map.CommonNormalizingMapper;
import com.run.ycl.utils.PbSequenceFileStreamOutput;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class YclMainNew {

    private static final Logger logger = LoggerFactory.getLogger(YclMainNew.class);
    private static Jedis jedis = null;

    public static void main(String[] args) {
        args = new String[] {
                "--input-topic", "ycl_ycl01",
                "--output-topic", "source_144_WA_SOURCE_0001",
                "--bootstrap.servers", "192.168.251.73:9092,192.168.38.36:9092,192.168.38.37:9092",
                "--zoookeeper.connect", "192.168.251.73:2888",
                "--group.id", "9",
                "--dbhost", "192.168.251.73",
                "--dbname", "testdb",
                "--username", "root",
                "--password", "root"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 9) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>" +
                    "--flink.partition-discovery.interval-millis <kafka partition discovery interval>" +
                    "--dbhost 192.168.251.73 --dbname testdb --username root --password root");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        String topic = parameterTool.getRequired("input-topic");
        //List<String> topicList = Arrays.asList(parameterTool.getRequired("input-topic").split(" "));
        logger.info("======topics: {}",topic);

        FlinkKafkaConsumer010<String> flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                Pattern.compile(topic),
                // new SimpleStringSchema(Charset.forName("GBK")),
                new SimpleStringSchema(),
                parameterTool.getProperties());
        flinkKafkaConsumer.setStartFromEarliest();
        //flinkKafkaConsumer.setStartFromGroupOffsets();

        DataStream<String> input = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<JSONObject> dataMapRes;

        //格式转化
        dataMapRes =  input.map(new CommonDataMappingMapper());

        //数据截断
        //dataMapRes = DataTruncation.dataTruncationHandle(dataMapRes);

        //数据归一化
        dataMapRes = dataMapRes.map(new CommonNormalizingMapper());

        //基于窗口数据去重
        dataMapRes = DuplicateHandleWithWindow.duplicateWithWindowHandle(dataMapRes);

        //数据校验
        dataMapRes = DataValidate.dataValidateHandle(dataMapRes);
        //dataMapRes.print();
        //布控
        dataMapRes = BK.bkHandle(dataMapRes);

        //结构化提取
        dataMapRes = dataMapRes.flatMap(new CommonFlatMapingMapper());

        //标准输出 protobuf + sequenceFile + snappy 输出到hdfs
        List<String> protocolList = new ArrayList<String>(); //流中所有数据的协议类型
        protocolList.add("WA_BASIC_0021");
        dataMapRes = PbSequenceFileStreamOutput.pbSequenceFileOutput(protocolList, dataMapRes);

        //MPP输出 bcp格式给五事入库Greenplum
//        new BKStreamOutput(dataMapRes, String.format("/%s/%s", "output_test_02",parameterTool.get("input-topic")));

        try {
          //  env.setParallelism(3);
            env.execute(parameterTool.get("input-topic"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
