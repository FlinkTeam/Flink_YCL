package com.run.ycl.sideout;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.CharUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CleanSideOutput extends ProcessFunction<JSONObject, JSONObject> {
    private OutputTag<JSONObject> emailOutputTag;
    private OutputTag<JSONObject> smsOutputTag;

    private static List<String> rubbishEmails = null;
    private static List<String> rubbishSms = null;

    public CleanSideOutput(OutputTag<JSONObject> emailOutputTag, OutputTag<JSONObject> smsOutputTag){
        this.emailOutputTag = emailOutputTag;
        this.smsOutputTag = smsOutputTag;
    }
    static {
        //初始化配置
        init1();
        init2();
    }
    private static void init1(){
        rubbishEmails = new ArrayList<>();
        rubbishEmails.add("yubing@kaixin001.com");
        rubbishEmails.add("gucciworld@news.gucci.com");
        rubbishEmails.add("2523019274@qq.com");
        rubbishEmails.add("jx3@kingsoft.com");
        rubbishEmails.add("domainacres@cox.net");
    }
    private static void init2(){
        rubbishSms = new ArrayList<>();
        rubbishSms.add("10086");
        rubbishSms.add("10010");
        rubbishSms.add("106589996400");
        rubbishSms.add("106550200492620");
        rubbishSms.add("10658258");
        rubbishSms.add("95188");
        rubbishSms.add("10656001");
    }
    @Override
    public void processElement(JSONObject valueJson, Context ctx, Collector<JSONObject> out)
            throws Exception {

        //是否是垃圾标志
        boolean isRubbish = false;
        if (valueJson !=null){
            //协议名
            String source_name = valueJson.getString("SOURCE_NAME");
            //判断协议名是否是配置中需要进行邮件过滤的协议名
            if ("WA_MFORENSICS_040300".equals(source_name)){
                //H030002字段对应的数据 需要进行和垃圾邮件列表进行对比
                String compare_value = valueJson.getString("H030002");
                //H030001字段对应的数据 需要判断是否是乱码
                String messy_value = valueJson.getString("H030001");
//                String messy_value = "据\uD87Eab的十\uD87E\uDCB1多块";
                //判断compare_value是否在垃圾邮件列表中 判断messy_value是否是乱码
                if (rubbishEmails.contains(compare_value) || isMessyCode(messy_value)){
                    //说明是垃圾邮件
                    isRubbish = true;
                    ctx.output(emailOutputTag,valueJson);
                }
            }
            //判断协议名是否是配置中需要进行邮件过滤的协议名，并且H040002值为01
            if ("WA_MFORENSICS_010700".equals(source_name)){
                //H040002值为01时,表示对方发送
                String messy_value = valueJson.getString("H040002");
                if ("01".equals(messy_value)){
                    //B070003字段对应的数据表示对方号码 需要判断是否在垃圾短信号码列表中
                    String compare_value = valueJson.getString("B070003");

                    //判断compare_value是否在垃圾短信号码列表中
                    if (rubbishSms.contains(compare_value)){
                        //说明是垃圾短信
                        isRubbish = true;
                        ctx.output(smsOutputTag,valueJson);
                    }
                }
            }
        }
        if (!isRubbish){
            out.collect(valueJson);
        }
    }
    /**
     * 判断字符是否是中文
     *
     * @param c 字符
     * @return 是否是中文
     */
    private boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
            return true;
        }
        return false;
    }

    /**
     * 判断字符串是否是乱码
     *
     * @param strName 字符串
     * @return 是否是乱码
     */
    private boolean isMessyCode(String strName) {
        // \s 匹配任何不可见字符，包括空格、制表符、换页符等等。等价于[ \f\n\r\t\v]。
        Pattern p = Pattern.compile("\\s*|t*|r*|n*");
        Matcher m = p.matcher(strName);
        String after = m.replaceAll("");
        //标点字符
        String temp = after.replaceAll("\\p{P}", "");
        char[] ch = temp.trim().toCharArray();
        for(char c : ch) {
            if (!CharUtils.isAsciiAlphanumeric(c)) {
                //如果数据是文件格式，且被转过编码，可能携带﻿﻿零宽空格，即\uFEFF，忽略
                if (!isChinese(c)) {
                    return true;
                }
            }
        }
        return false;
    }
}
