package com.run.ycl.map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.ConfigService;
import com.run.ycl.utils.DmUtils;
import com.run.ycl.utils.JsonUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * 通用格转
 * DataMapping.xml
 */
public class CommonDataMappingMapper implements MapFunction<String, JSONObject> {
    private static final Logger LOG = LoggerFactory.getLogger(CommonDataMappingMapper.class);

    public CommonDataMappingMapper() {
    }

    @Override
    public JSONObject map(String value) throws Exception {
        JSONObject json = null;
        try {
            json = JSONObject.parseObject(value);
        }catch (Exception e) {
            e.printStackTrace();
            System.out.println(value);
        }

        if (json != null) {
            LOG.info("map result :{}", json.toJSONString());
            String sourceName = json.getString("SOURCE_NAME");
            String data_source = json.getString("DATA_SOURCE").toUpperCase();
            String sdataset = json.getString("SDATASET");

            JSONObject dataMappingJson = (JSONObject) JSONObject.parse(ConfigService.getString(
                    data_source + sdataset+ "_2_" + sourceName));

//            JSONArray normalizedFieldArray = dataMappingJson.getJSONArray("NormalizedField");
            JSONArray normalizedFieldArray = JsonUtils.getJSONArray(dataMappingJson, "NormalizedField");
            final JSONObject finalJson = json;
            normalizedFieldArray.forEach(normalizedField -> {
                JSONObject normalizedFieldJson = (JSONObject) normalizedField;
                handle(finalJson, normalizedFieldJson);
            });
        }

        //设置全局唯一的dataID
        json.put("DATA_ID", getDataID());

        return json;
    }

    /**
     * 由25位组成
     * 前16位每一位为十进制数字
     * 后9位为16进制数字
     * 10位时间戳+6位数据来源的省市编码+ 9位UUID字段（从后往前截取9位）
     */
    private String getDataID() {
        Long second = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"));
        String uuId = UUID.randomUUID().toString();
        String cityCode = ConfigService.getString("dpp_city_id");
        return String.valueOf(second) + cityCode + uuId.substring(uuId.length() - 8);
    }

    private void handle(JSONObject value, JSONObject normalizedField) {
        JSONObject functionJson = normalizedField.getJSONObject("Expression");
        if (null != functionJson) {
            String functionName = functionJson.getString("Function");
            LOG.info("dmfunction :{}", functionName);
            switch (functionName) {
                case "empty" :
                    break;
                case "dm_copy" :
                    DmUtils.dmCopy(value, normalizedField);
                    break;
                case "dm_map" :
                   // DmUtils.dmMap(value, functionJson, new HashMap<String, String>());
                    break;
                case "dm_assignment" :
                    DmUtils.dmAssignment(value, normalizedField);
                    break;
                case "dm_time" :
                    DmUtils.dmTime(value, normalizedField);
                    break;
                default:
                    break;
            }
        }
    }
}
