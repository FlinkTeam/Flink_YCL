import com.alibaba.fastjson.JSONObject;
import com.run.ycl.DataTruncation;
import com.run.ycl.bk.BKConfigInit;
import com.run.ycl.common.ConfConstants;
import com.run.ycl.filter.CommonDataValidateFilter;
import com.run.ycl.keyby.AJRelevanceSelector;
import com.run.ycl.keyby.DataKeySelector;
import com.run.ycl.map.*;
import com.run.ycl.sideout.BKSideOutputFunc;
import com.run.ycl.sideout.CleanSideOutput;
import com.run.ycl.sideout.InvalidSideOutputFunc;
import com.run.ycl.split.PreRelatedSplitter;
import com.run.ycl.split.SplitFunc;
import com.run.ycl.split.UniqueSplit;
import com.run.ycl.utils.BcpStreamOutput;
import com.run.ycl.utils.RedisUtils;
import com.run.ycl.utils.RowStreamOutput;
import com.run.ycl.window.AJRelevanceWindow;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.Types;
import java.util.*;

public class YclMain {

    private static final Logger logger = LoggerFactory.getLogger(YclMain.class);
    private static Jedis jedis = null;

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

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        FlinkKafkaConsumer010<String> flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-topic"),
                // new SimpleStringSchema(Charset.forName("GBK")),
                new SimpleStringSchema(),
                parameterTool.getProperties());
        flinkKafkaConsumer.setStartFromEarliest();
        //flinkKafkaConsumer.setStartFromGroupOffsets();

        DataStream<String> input = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<JSONObject> dataMapRes;

        if (true) {
            //格式转化
            dataMapRes =  input.map(new CommonDataMappingMapper());
        }

        dataMapRes = DataTruncation.dataTruncationHandle(dataMapRes);

        if (true) {
            //split + keyBy + window 方式去重
            jedis = RedisUtils.getRedisClient();
            String windowSize = jedis.get(ConfConstants.UNIQUE_STRUCT_CONFIG + ConfConstants.UNIQUE_CONFIG_INTERVAL);
            if (!StringUtils.isBlank(windowSize)) {
                windowSize = "10";
            }
            String dataSet = jedis.get(ConfConstants.UNIQUE_STRUCT_CONFIG + ConfConstants.UNIQUE_STRUCT_DATA_SET);
            if (dataSet != null) {
                String[] array = dataSet.split(",");
                HashMap<String, String> dataSetMap = new HashMap<>();
                for (String sourceName : array) {
                    String sourceStr = jedis.get(ConfConstants.UNIQUE_STRUCT_CONFIG + sourceName);
                    dataSetMap.put(sourceName, sourceStr);
                }
                SplitStream<JSONObject> split = dataMapRes.split(new UniqueSplit(dataSet));
                DataStream<JSONObject> normal = split.select("normal");
                DataStream<JSONObject> unique = split.select("unique");
                SingleOutputStreamOperator<JSONObject> apply = unique.keyBy(new DataKeySelector(dataSetMap))
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(Long.valueOf(windowSize))))
                        .apply(new WindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {
                                Iterator<JSONObject> iterator = input.iterator();
                                while (iterator.hasNext()) {
                                    out.collect(iterator.next());
                                    break;
                                }
                            }
                        });
                dataMapRes = normal.union(apply).map(e -> e);

            }

        }

        final OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("err"){};
        dataMapRes = dataMapRes.process(new InvalidSideOutputFunc(outputTag,new CommonDataValidateFilter()){});


        //校验失败的数据
        DataStream<JSONObject> invalidateDataRow = dataMapRes.getSideOutput(outputTag);

        dataMapRes = dataMapRes.map(x->x);
        //案件重要信息关联
//        if (true) {
        if (false) {

            String windowSize = "10";
            //关联字段 I050008 取证id
            String relevanceField = "I050008";

            String relevance = "WA_MFORENSICS_010100";
            //需要关联的协议
            List<String> relevance_left = new ArrayList<>();
            relevance_left.add("WA_MFORENSICS_010300");
            relevance_left.add("WA_MFORENSICS_010600");
            relevance_left.add("WA_MFORENSICS_010700");
            relevance_left.add("WA_MFORENSICS_010800");
            //需要回填的协议
            List<String> relevance_Right = new ArrayList<>();
            relevance_Right.add("WA_MFORENSICS_010100");
            relevance_Right.add("WA_MFORENSICS_010300");
            relevance_Right.add("WA_MFORENSICS_010400");
            relevance_Right.add("WA_MFORENSICS_010600");
            relevance_Right.add("WA_MFORENSICS_010700");

            //根据配置解析出关联数据集和被回填的数据集，它们中都有关联字段
            ArrayList<String> relevances = new ArrayList<>();
            relevances.addAll(relevance_left);
            relevances.addAll(relevance_Right);
            relevances.add(relevance);


            SplitStream<JSONObject> relevanceSplit = dataMapRes.split(new UniqueSplit(relevances.toString()));

            DataStream<JSONObject> relevanceNormal = relevanceSplit.select("normal");
            DataStream<JSONObject> relevanceUnique = relevanceSplit.select("unique");
            SingleOutputStreamOperator<JSONObject> relevanceApply = relevanceUnique.keyBy(new AJRelevanceSelector(relevanceField))
            .window(TumblingProcessingTimeWindows.of(Time.seconds(Long.valueOf(windowSize))))
            .apply(new AJRelevanceWindow());
            dataMapRes = relevanceNormal.union(relevanceUnique).map(e -> e);
        }
//        if (true) {
        if (false) {
            //进行分流
            final OutputTag<JSONObject> emailOutputTag = new OutputTag<JSONObject>("rubbishEmail"){};

            final OutputTag<JSONObject> smsOutputTag = new OutputTag<JSONObject>("rubbishSms"){};

            dataMapRes = dataMapRes.process(new CleanSideOutput(emailOutputTag, smsOutputTag));

            //垃圾邮件
            DataStream<String> rubbishEmailStream = dataMapRes.getSideOutput(emailOutputTag).map(new PaseJsonMapper());
            //垃圾短信
            DataStream<String> rubbishSmsStream = dataMapRes.getSideOutput(smsOutputTag).map(new PaseJsonMapper());

            final OutputTag<String> truncaOutputTag = new OutputTag<String>("dataTruncation"){};
            //进行数据截断
          //  dataMapRes = dataMapRes.process(new TruncationSideOutput(truncaOutputTag));

            //得到侧输出流（数据超长字段）
            DataStream<String> longValStream = dataMapRes.getSideOutput(truncaOutputTag);

            //将垃圾过滤和数据超长存放到hdfs中
            String pathTimeFormat = "yyyy-MM-dd--HH-mm";
            String rubbishEmaliPath = "yclTest/rubbishEmail";
            String rubbishSmsPath = "yclTest/rubbishSms";
            String longDataPath = "yclTest/longD";
            long batchSize = 1024 * 1024 * 400;

            BucketingSink<String> cleanEmailByHDFS = new BucketingSink<>(rubbishEmaliPath);
            cleanEmailByHDFS.setBucketer(new DateTimeBucketer<>(pathTimeFormat));
            cleanEmailByHDFS.setBatchSize(batchSize);

            BucketingSink<String> cleanSmsByHDFS = new BucketingSink<>(rubbishSmsPath);
            cleanSmsByHDFS.setBucketer(new DateTimeBucketer<>(pathTimeFormat));
            cleanSmsByHDFS.setBatchSize(batchSize);

            BucketingSink<String> cleanLongByHDFS = new BucketingSink<>(longDataPath);
            cleanLongByHDFS.setBucketer(new DateTimeBucketer<>(pathTimeFormat));
            cleanLongByHDFS.setBatchSize(batchSize);

            Configuration hdfsConfig = new Configuration();
            hdfsConfig.set("fs.defaultFS", "hdfs://192.168.251.73:8020");

            cleanEmailByHDFS.setFSConfig(hdfsConfig);
            cleanSmsByHDFS.setFSConfig(hdfsConfig);
            cleanLongByHDFS.setFSConfig(hdfsConfig);
            longValStream.print();
            rubbishEmailStream.addSink(cleanEmailByHDFS);
            rubbishSmsStream.addSink(cleanSmsByHDFS);
            longValStream.addSink(cleanLongByHDFS);
        }

        //数据归一化
        dataMapRes = dataMapRes.map(new CommonNormalizingMapper());


        //关联
        if (true) {
          
        //if (false) {
            SplitStream<JSONObject> soureSplitStream = dataMapRes.split(new PreRelatedSplitter());
            DataStream<JSONObject> s500DataStream = soureSplitStream.select("WA_MFORENSICS_010500");
            DataStream<JSONObject> s400DataStream = soureSplitStream.select("WA_MFORENSICS_010400");
            DataStream<JSONObject> joinStream = s400DataStream.join(s500DataStream)
                .where(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("I050008") + value.getString("J020011");
                    }
                })
                .equalTo(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("I050008") + value.getString("J020011");
                    }
                })
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new JoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public JSONObject join(JSONObject first, JSONObject second) throws Exception {
                        first.put("I010051", second.getString("I010051"));
                        first.put("B020017", second.getString("B020017"));
                        first.put("B070003", second.getString("B070003"));
                        return first;
                    }
                });
        }

        //布控
        Map<String,OutputTag<JSONObject>> bkOutputTag = BKConfigInit.bkOutputTag;
        dataMapRes = dataMapRes.process(new BKSideOutputFunc(bkOutputTag){});
        for (Map.Entry<String,OutputTag<JSONObject>> entry : bkOutputTag.entrySet()){
            DataStream<JSONObject> bkOutputStream = dataMapRes.getSideOutput(entry.getValue());
//            new RowStreamOutput(bkOutputStream, String.format("/%s/%s","BKOutput",entry.getKey()));
            //bkOutputStream.print();//just for test
        }

        //结构化提取
        if (true) {
            dataMapRes = dataMapRes.flatMap(new CommonFlatMapingMapper());
        }

        //数据标识 根据手机号 将归属地及运营商回填
        if (true) {
            dataMapRes = dataMapRes.flatMap(new CommonDataMapper());
        }

        //input.print();
        //dataMapRes.print();

        Jedis jedis = RedisUtils.getRedisClient();
        Map<String,String> insertSqlMap = jedis.hgetAll("insert_sql");
        Map<String,String> insertSqlMapErr = jedis.hgetAll("insert_sql_for_error_data");
        jedis.close();
        Set<String> protocols = new HashSet<>();
        for (String key : insertSqlMap.keySet())
            protocols.add(key);

        SplitStream<JSONObject> splitStream = dataMapRes.split(new SplitFunc(protocols));

        if(insertSqlMap != null && !insertSqlMap.isEmpty()) {
            Set<String> protocolSet = insertSqlMap.keySet();
            for (String protocol : protocolSet) {
                String insertSql = insertSqlMap.get(protocol);
                String insertSqlErr = insertSqlMapErr.get(protocol);
                String[] str = insertSql.split("\\?");
                int count = str.length - 1;
                int[] rowTypeInfo = new int[count];
                for (int i = 0; i < count; i++)
                    rowTypeInfo[i] = Types.VARCHAR;

                DataStream<JSONObject> protocolStream = splitStream.select(protocol);

/*
                final OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("err_"+protocol){};
                dataMapRes = protocolStream.process(new InvalidSideOutputFunc(outputTag,new CommonDataValidateFilter()){});

                //校验失败的数据
                DataStream<Row> invalidateDataRow = dataMapRes.getSideOutput(outputTag).map(new CreateRowMap(insertSqlErr));

                statisticStream = statisticStream.union(StatisticStream.getStatistcStream(dataMapRes,protocol,"gsjy"));
*/

                //正常数据
                //DataStream<Row> protocolRowStream = dataMapRes.map(new CreateRowMap(insertSql));


                DataStream<String> protocolBcpStream = protocolStream.map(new CreateBcpMap(insertSql));

                new BcpStreamOutput(protocolBcpStream, String.format("/%s/%s","DataOutput", protocol));

/*                try {
                    JDBCOutputFormat sink = JDBCOutputFormat.buildJDBCOutputFormat()
                            .setDrivername("com.mysql.jdbc.Driver")
                            .setDBUrl(String.format("jdbc:mysql://%s/%s?rewriteBatchedStatements=true", parameterTool.get("dbhost"), parameterTool.get("dbname")))
                            .setUsername(parameterTool.get("username"))
                            .setPassword(parameterTool.get("password"))
                            .setQuery(insertSql)
                            .setSqlTypes(rowTypeInfo)
                            .setBatchInterval(2000)
                            .finish();
                    JDBCOutputFormat sink2 = JDBCOutputFormat.buildJDBCOutputFormat()
                            .setDrivername("com.mysql.jdbc.Driver")
                            .setDBUrl(String.format("jdbc:mysql://%s/%s?rewriteBatchedStatements=true", parameterTool.get("dbhost"), parameterTool.get("dbname")))
                            .setUsername(parameterTool.get("username"))
                            .setPassword(parameterTool.get("password"))
                            .setQuery(insertSqlErr)
                            .setSqlTypes(rowTypeInfo)
                            .setBatchInterval(2000)
                            .finish();

                    invalidateDataRow.writeUsingOutputFormat(sink2).name("ERR_"+protocol).setParallelism(1);
                    protocolRowStream.writeUsingOutputFormat(sink).name(protocol).setParallelism(1);
                }catch (Exception e){
                    e.printStackTrace();
                }
                */
            }
        }else {
            System.out.println("get mysql tableinfo from redis error!");
        }

        try {
          //  env.setParallelism(3);
            env.execute("Kafka 0.10 Example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
