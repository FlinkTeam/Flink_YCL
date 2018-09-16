package com.run.ycl.window;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class AJRelevanceWindow implements WindowFunction<JSONObject, JSONObject, String, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(AJRelevanceWindow.class);
    @Override
    public void apply(String s, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {

        Iterator<JSONObject> iterator = input.iterator();
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

        List<JSONObject> relevanceValues = new LinkedList<>();
        //身份证号
        String card = null;
        //手机号
        String phone = null;
        while (iterator.hasNext()) {
            JSONObject valueJson = iterator.next();
            if (valueJson == null) {
                continue;
            }
            String sourceName = valueJson.getString("SOURCE_NAME");

            if (relevance.equals(sourceName)) {
                //类型
                String b030004 = valueJson.getString("B030004");
                //身份证号
                String b030005 = valueJson.getString("B030005");
                if ("111".equals(b030004) && b030005!=null && b030005.length()>0){
                    //如果是b030004类型是身份证号，但是这条数据的 b030005 为空
                    //只回填手机号，不回填身份证和类型
                    //如果数据 b030005 不为空，那么回填身份证和类型，然后等待回填手机号
                    if (card == null){
                        card = b030005;
                    }
                    //回填类型和身份证号
                    valueJson.put("Z0021IB","111");
                    valueJson.put("Z0021TA",card);
                }
                if (phone != null) {
                    //回填手机号
                    valueJson.put("Z002102", phone);
                    //将这条数据直接输出
                    out.collect(valueJson);
                }else {
                    //放到relevanceMap中的WA_MFORENSICS_010100只需要回填phone即可
                    relevanceValues.add(valueJson);
                }
            }
            if (relevance_left.contains(sourceName)) {

                //如果在relevance_left中的，需要在数据中找到phone

                if (phone == null) {
                    String b020005 = valueJson.getString("B020005");
                    if (b020005 != null && b020005.length()>0 && phone == null){
                        phone = b020005;
                    }
                }
                //如果该协议不在需要回填的协议中，那么直接collect
                if (!relevance_Right.contains(sourceName)) {
                    out.collect(valueJson);
                }
            }
            if (relevance_Right.contains(sourceName) && !relevance.equals(sourceName)){
                //进行回填
                boolean isPut = false;
                if (card != null) {
                    if ("WA_MFORENSICS_010300".equals(sourceName)) {
                        valueJson.put("Z0021TA", card);
                        out.collect(valueJson);
                    }else {
                        if (phone != null) {
                            valueJson.put("Z0021TA", card);
                            valueJson.put("Z002102", phone);
                            out.collect(valueJson);
                        }else {
                            isPut = true;
                        }
                    }
                }else {
                    isPut = true;
                }
                if (isPut) {
                    relevanceValues.add(valueJson);
                }
            }
        }
        LOG.error("开始时间"+window.getStart());
        LOG.error("结束时间"+window.getEnd());
        LOG.error("缓冲了"+relevanceValues.size()+"条数据");
        for (Iterator<JSONObject> allValues = relevanceValues.iterator();allValues.hasNext();) {
            JSONObject value = allValues.next();
            String sourceName = value.getString("SOURCE_NAME");
            if (sourceName.equals(relevance)) {
                value.put("Z002102", phone);
                out.collect(value);
            }else if (sourceName.equals("WA_MFORENSICS_010300")) {
                value.put("Z0021TA", card);
                out.collect(value);
            }else {
                value.put("Z0021TA", card);
                value.put("Z002102", phone);
                out.collect(value);
            }
        }
    }
}
