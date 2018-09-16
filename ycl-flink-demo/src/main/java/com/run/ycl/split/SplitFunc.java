package com.run.ycl.split;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.RedisUtils;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class SplitFunc implements OutputSelector<JSONObject>{
    Set<String> protocolSet;
    public SplitFunc(Set<String> protocolSet){
       this.protocolSet =  protocolSet;
    }
    @Override
    public Iterable<String> select(JSONObject value) {

        List<String> output = new ArrayList();
        String protocol="";
        if(value != null)
            protocol = value.getString("SOURCE_NAME");
//        if(protocol.equals("WA_MFORENSICS_020700")) {
//            output.add("WA_MFORENSICS_020700");
//        }
        for (String item : protocolSet) {
            if (item.equals(protocol))
                output.add(item);
        }
        return output;
    }
}
