package com.run.ycl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *  提取运营商规则信息
 */
public class RuleHandler {
    private static final Logger logger = LoggerFactory.getLogger(RuleHandler.class);
    //定义一个map，其中Phone为Key，规则信息为value
    private static  Map<String, String> phoneMap = new HashMap<String, String>();
    //定时加载配置文件的标识
    private static Connection connection = null;
    private static PreparedStatement ps = null;

    static {
        load();
    }

    public static Map getRuleMap() {
        if (phoneMap == null){
            load();
        }
        return phoneMap;
    }

    /**
     * 加载数据模型。
     */
    public static synchronized void load() {
        if (phoneMap == null) {
            phoneMap = loadRuleMap();
        }
    }

    /**
     *  封装应用与规则的map
     * @return
     */
    private static Map<String, String> loadRuleMap() {
        String driver = "com.mysql.jdbc.Driver";
        String url ="jdbc:mysql://192.168.251.73:3306/rule";
        String username = "root";
        String password = "root";
        long start = System.currentTimeMillis();
        //加载驱动
        try {
            Class.forName(driver);
            //创建连接
            connection = DriverManager.getConnection(url,username,password);
            String sql = "select phone_pre, op, city from t_phone_rule";
            ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String phone_pre = resultSet.getString("phone_pre");
                String op = resultSet.getString("op");
                String city = resultSet.getString("city");
                phoneMap.put(phone_pre, op + "|" + city);
            }
            phoneMap = loadRuleMap();
            logger.info("手机号段归属地、运营商的映射加载完成，耗时："+ (System.currentTimeMillis() - start));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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

        return phoneMap;
    }


}
