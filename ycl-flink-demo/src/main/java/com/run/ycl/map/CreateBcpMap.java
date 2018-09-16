package com.run.ycl.map;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CreateBcpMap implements MapFunction<JSONObject, String> {
    private String insertSql;
    public CreateBcpMap(String insertSql){
        this.insertSql = insertSql;
    }
    @Override
    public  String map(JSONObject value) throws Exception {
        List<String> list = getFields(insertSql);
        StringBuilder stringBuilder = new StringBuilder();
        for(String field : list) {
            stringBuilder.append(value.getString(field)+'\t');
        }
        stringBuilder.replace(stringBuilder.length()-1,stringBuilder.length(),"\n");
        return stringBuilder.toString();
    }

    public List<String> getFields(String sql){
        List<String> list = new ArrayList<>();
        if(sql!= null && !sql.isEmpty()){
           int beg = sql.indexOf('(') ;
           int end = sql.indexOf(')',beg+1);
           String fieldStr = sql.substring(beg+1,end);
           String [] fields = fieldStr.split(",");
            Collections.addAll(list,fields);
        }
        return list;
    }

}
