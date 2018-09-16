package com.run.ycl.map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.JsonUtils;
import com.run.ycl.utils.RedisUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by jyt on 2018/8/10.
 */
public class CommonDataMapper extends RichFlatMapFunction<JSONObject,JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(CommonDataMapper.class);
    //定义一个map，其中Phone为Key，规则信息为value
    public static final Map<String, String> ruleMap = new HashMap<String, String>();
    private static Connection connection = null;
    private static PreparedStatement ps = null;
    private Jedis jedis = null;
    private static final String SOURCE_NAME = "SOURCE_NAME";
    private static final String PHONE_RULE_CONFIG_PRE = "PhoneRuleConfig_";
    private static final String PHONE_RULE_CONFIG_ITEM = "Item";
    private static final String PHONE_RULE_CONFIG_ITEM_NODE_NAME = "Name";
    private static final String PHONE_RULE_CONFIG_ITEM_CITY = "city_field";
    private static final String PHONE_RULE_CONFIG_ITEM_OP = "op_field";
    private static final String PHONE_RULE_CONFIG_ITEM_PHONE = "phone_field";
    private static final String PHONE_RULE_CONFIG_ITEM_NODE_VALUE = "Value";

    public CommonDataMapper() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = RedisUtils.getRedisClient();
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.251.73:3306/rule";
        String username = "root";
        String password = "root";
        //加载驱动
        try {
            Class.forName(driver);
            //创建连接
            connection = DriverManager.getConnection(url, username, password);
            String sql = "select phone_pre, op, city from t_phone_rule";
            ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            long start = System.currentTimeMillis();
            while (resultSet.next()) {
                String phone_pre = resultSet.getString("phone_pre");
                String op = resultSet.getString("op");
                String city = resultSet.getString("city");
                ruleMap.put(phone_pre, op + "|" + city);
            }
            logger.info("手机号段归属地、运营商的映射加载完成，耗时：" + (System.currentTimeMillis() - start));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis != null) {
            jedis.close();
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
        try {
            String sourceName = value.getString(SOURCE_NAME);
            String sourceStr = jedis.get(PHONE_RULE_CONFIG_PRE + sourceName);

            JSONObject dataJson = JSONObject.parseObject(sourceStr);
            JSONArray items= JsonUtils.getJSONArray(dataJson, PHONE_RULE_CONFIG_ITEM);
            Iterator<Object> iterator = items.iterator();
            String cityField = null;
            String opFiled = null;
            String phoneField = null;
            //提取配置文件内容
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                String nameVal = jsonObject.getString(PHONE_RULE_CONFIG_ITEM_NODE_NAME);
                if (PHONE_RULE_CONFIG_ITEM_CITY.equals(nameVal)) {
                    cityField = jsonObject.getString(PHONE_RULE_CONFIG_ITEM_NODE_VALUE);
                } else if (PHONE_RULE_CONFIG_ITEM_OP.equals(nameVal)) {
                    opFiled = jsonObject.getString(PHONE_RULE_CONFIG_ITEM_NODE_VALUE);
                } else if (PHONE_RULE_CONFIG_ITEM_PHONE.equals(nameVal)) {
                    phoneField = jsonObject.getString(PHONE_RULE_CONFIG_ITEM_NODE_VALUE);
                }
            }
            String cityVal = value.getString(cityField);
            String opVal = value.getString(opFiled);
            String phoneVal = value.getString(phoneField);

            if (phoneVal != null && phoneVal.length() > 10) {
                if (phoneVal.startsWith("86") && phoneVal.length() > 12) {
                    phoneVal = phoneVal.substring(2, 9);
                } else {
                    phoneVal = phoneVal.substring(0, 7);
                }
                if (cityVal == null && ruleMap != null && cityField != null) {
                    //phoneMap.put(phone_pre, op + "|" + city)
                    value.put(cityField,ruleMap.get(phoneVal).split("\\|")[1].trim());
                }
                if (opVal == null && ruleMap != null && opFiled != null) {
                    //phoneMap.put(phone_pre, op + "|" + city)
                    value.put(opFiled,ruleMap.get(phoneVal).split("\\|")[0].trim());
                }
            }
            out.collect(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
