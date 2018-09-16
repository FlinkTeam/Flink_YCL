package com.run.ycl.split;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

//将各种协议的数据进行分类，方便后续取指定的流进行关联
public class PreRelatedSplitter implements OutputSelector<JSONObject> {
    @Override
    public Iterable<String> select(JSONObject value) {
        List<String> output = new ArrayList<String>();
        String sourceName = value.getString("SOURCE_NAME");
        output.add(sourceName);
        return output;
    }
}
