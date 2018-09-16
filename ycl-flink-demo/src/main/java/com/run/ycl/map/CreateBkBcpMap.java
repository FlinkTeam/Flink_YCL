package com.run.ycl.map;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.bk.BKConfigInit;
import com.run.ycl.bk.FieldProperties;
import com.run.ycl.bk.ProtocolFields;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CreateBkBcpMap implements MapFunction<JSONObject, String> {
    private String protoName;
    public CreateBkBcpMap(String protocolName){
        this.protoName = protocolName;
    }
    @Override
    public  String map(JSONObject value) throws Exception {
        List<FieldProperties> list = ProtocolFields.protocolFieldsMap.get(protoName);
        StringBuilder stringBuilder = new StringBuilder();
        for(FieldProperties field : list) {
            String val = value.getString((field.getElemenId()));
            if(null == val)
                val="";
            stringBuilder.append(val+'\t');
        }
        stringBuilder.replace(stringBuilder.length()-1,stringBuilder.length(),"\n");
        return stringBuilder.toString();
    }
}
