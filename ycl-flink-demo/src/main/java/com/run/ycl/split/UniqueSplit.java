package com.run.ycl.split;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.common.ConfConstants;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

/**
 * 将去重的协议分流.
 */
public class UniqueSplit implements OutputSelector<JSONObject> {
    private String dataSet = "";

    public UniqueSplit() {
    }
    public UniqueSplit(String dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public Iterable<String> select(JSONObject value) {
        List<String> output = new ArrayList<String>();
        String sourceName = value.getString(ConfConstants.SOURCE_NAME);
        if (dataSet.contains(sourceName)) {
            output.add("unique");
        } else {
            output.add("normal");
        }
        return output;
    }
}


