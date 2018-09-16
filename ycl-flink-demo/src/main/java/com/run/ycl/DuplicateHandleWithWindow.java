package com.run.ycl;

import com.alibaba.fastjson.JSONObject;
import com.run.ycl.common.ConfConstants;
import com.run.ycl.keyby.DataKeySelector;
import com.run.ycl.split.UniqueSplit;
import com.run.ycl.utils.ConfigService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by jianghui on 18-8-29.
 */
public class DuplicateHandleWithWindow {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateHandleWithWindow.class);

    public static SingleOutputStreamOperator<JSONObject>  duplicateWithWindowHandle(SingleOutputStreamOperator<JSONObject> dataMapRes) {
        //split + keyBy + window 方式去重

        String windowSize = ConfigService.getString(ConfConstants.UNIQUE_STRUCT_CONFIG + ConfConstants.UNIQUE_CONFIG_INTERVAL);
        if (!StringUtils.isBlank(windowSize)) {
            windowSize = "10";
        }
        String dataSet = ConfigService.getString(ConfConstants.UNIQUE_STRUCT_CONFIG + ConfConstants.UNIQUE_STRUCT_DATA_SET);
        if (logger.isDebugEnabled()) {
            logger.debug("pre Data DuplicateHandle config: Interval:{}, dataSet:{}",
                    windowSize, dataSet);
        }

        if (dataSet != null) {
            String[] array = dataSet.split(",");
            HashMap<String, String> dataSetMap = new HashMap<>();
            for (String sourceName : array) {
                String sourceStr = ConfigService.getString(ConfConstants.UNIQUE_STRUCT_CONFIG + sourceName);
                dataSetMap.put(sourceName, sourceStr);
            }
            SplitStream<JSONObject> split = dataMapRes.split(new UniqueSplit(dataSet));
            DataStream<JSONObject> normal = split.select("normal");
            DataStream<JSONObject> unique = split.select("unique");
            SingleOutputStreamOperator<JSONObject> apply = unique.keyBy(new DataKeySelector(dataSetMap))
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(Long.valueOf(windowSize))))
                    .apply(new WindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                        @Override
                        public void apply(String s, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {
                            Iterator<JSONObject> iterator = input.iterator();
                            int i = 0;
                            while (iterator.hasNext()) {
                                JSONObject next = iterator.next();
                                //只取一条
                                if (i++ == 0) {
                                    out.collect(next);
                                } else {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("pre Data DuplicateHandle data: {}", next.toJSONString());
                                    }
                                }
                            }
                        }
                    });
            dataMapRes = normal.union(apply).map(e -> e);

        }
        return dataMapRes;
    }
}
