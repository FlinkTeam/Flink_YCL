package com.run.ycl.map;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.*;

public class CreateRowMap implements MapFunction<JSONObject, Row> {
    private String insertSql;
    public CreateRowMap(String insertSql){
        this.insertSql = insertSql;
    }
    @Override
    public  Row map(JSONObject value) throws Exception {

       int index = 0;
       List<String> list = getFields(insertSql);
       Row row = new Row(list.size());
       for(String field : list) {
           row.setField(index, value.getString(field));
           index++;
           //RowTypeInfo rowTypeInfo = new RowTypeInfo(types,names);
       }
        return row;
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
