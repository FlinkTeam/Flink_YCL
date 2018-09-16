package com.run.ycl.sideout;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Set;

public class TruncationSideOutput extends ProcessFunction<JSONObject, JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(TruncationSideOutput.class);

    private OutputTag<JSONObject> outputTag;
    private JSONObject protocolJsons;

    public TruncationSideOutput(OutputTag<JSONObject> outputTag, JSONObject protocolJsons){
        this.outputTag = outputTag;
        this.protocolJsons = protocolJsons;
    }

    @Override
    public void processElement(JSONObject valueJson, Context ctx,Collector<JSONObject> out)
            throws Exception {
        boolean isTrun;
        JSONObject reJson = new JSONObject();
        if (valueJson != null){
            //协议名
            String source_name = valueJson.getString("SOURCE_NAME");
            String dataId = valueJson.getString("DATA_ID");

            JSONObject fieldsJson = protocolJsons.getJSONObject(source_name);

            if (logger.isDebugEnabled()) {
                logger.debug("pre Data Truncation config: source_name:{}, dataId:{}, fieldsJson:{}",
                        new String[] {source_name, dataId, fieldsJson.toJSONString()});
            }

            if (logger.isDebugEnabled()) {
                logger.debug("pre Data Truncation data: {}", valueJson.toJSONString());
            }

            if (fieldsJson != null) {
                Set<String> keySet = valueJson.keySet();
                for (String elementId : keySet) {
                    if (fieldsJson.containsKey(elementId)){
                        JSONObject fieldJson = fieldsJson.getJSONObject(elementId);

                        boolean beQueried = Boolean.valueOf(fieldJson.getString("BeQueried"));

                        //标准长度
                        int standard_len = Integer.valueOf(fieldJson.getString("ValueLength"));

                        //真实数据
                        String value = valueJson.getString(elementId);

                        try {
                            HashMap<String, String> map = trunca(value, standard_len, ".");
                            String truncationVal = map.get("truncationVal");
                            isTrun = Boolean.valueOf(map.get("isTrun"));
                            if (isTrun){
                                valueJson.put(elementId, truncationVal);
                                if (beQueried) {
                                    reJson.put(elementId, value);
                                }
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Data Truncation data: elementId:{}, before value:{}, after value:{}", elementId, value, truncationVal);
                                }
                            }
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }
                reJson.put("DATA_ID", dataId);
                reJson.put("SOURCE_NAME", source_name);
            }
        }
        out.collect(valueJson);
        if (reJson.size() > 2) {
            ctx.output(outputTag, valueJson);
        }
    }
    /**
     * 遍历字符的形式去截断字符
     * @param value 需要截断的数据
     * @param standard_len 字段标准长度
     * @param split_flag 如果数据大于标准长度，末尾加的截断标志字符
     * @return 截断后的字符
     */
    public static HashMap<String, String> trunca(String value, int standard_len, String split_flag) throws UnsupportedEncodingException {
        return trunca(value, standard_len, split_flag, "utf-8");
    }

    /**
     * 遍历字符的形式去截断字符
     * @param value 需要截断的数据
     * @param standard_len 字段标准长度
     * @param split_flag 如果数据大于标准长度，末尾加的截断标志字符
     * @param charname 字符编码 默认utf-8
     * @return 截断后的字符
     */
    public static HashMap<String, String> trunca(String value, int standard_len, String split_flag, String charname) throws UnsupportedEncodingException {

        HashMap<String, String> map = new HashMap<>();
        String truncationVal = value;
        //长度是否超标标志 1：超标 0：没超标
        String flag = null;
        if (value != null){
            if (split_flag == null){
                split_flag = "..";
            }
            byte[] valBytes = null;
            if (charname == null || "utf-8".equals(charname)){
                valBytes = value.getBytes("utf-8");
            }
            //真实数据的长度
            int value_len = valBytes.length;
            if (value_len > standard_len){
                //如果数据长度>标准长度
                //截断标志字符的长度
                int split_flag_len = split_flag.getBytes().length;
                //真实数据截断后的的长度
                int rel_len = standard_len - split_flag_len;
                if (rel_len > 0){
                    truncationVal = getTruncation(value, rel_len, valBytes) + split_flag;
                }else{
                    //如果截断标志字符长度大于等于标准，那么不增加截断标志字符
                    truncationVal = getTruncation(value, standard_len, valBytes);
                }
                flag = "true";
            }else{
                truncationVal = value;
                flag = "false";
            }
        }
        map.put("truncationVal", truncationVal);
        map.put("isTrun", flag);
        return map;

    }
    private static String getTruncation(String value, int len, byte[] valBytes){

        String rel = null;
        if (value != null && len > 0 && valBytes!=null){
            int valueLen = value.length();
            char[] valChars = new char[valueLen];
            value.getChars(0,valueLen,valChars,0);
            int valBytesIndex = 0;
            for (int i = 0; i < valueLen; i++){

                char valChar = valChars[i];

                int valCharInt = valChar;

                byte valByte = valBytes[valBytesIndex];

                if ((valByte & 0xFF) > 0x80){
                    //判断是否是高位
                    if (55296 < valCharInt && 56319 > valCharInt){
                        //表示高位
                        //和后一个字符合并，判断是否是4位字节，
                        String high = new String(new char[]{valChar, valChars[i + 1]});
                        if (high.getBytes().length==4){
                            if ((valBytesIndex + 4) > len){
                                break;
                            }
                            i ++;
                            valBytesIndex = valBytesIndex + 4;
                        }else{
                            //如果不是4位，那么获取这个字符的字节数，按字节数增加valBytesIndex
                            String noHigh = new String(new char[]{valChar});
                            int noHighLen = noHigh.getBytes().length;
                            if ((valBytesIndex + noHighLen) > len){
                                break;
                            }
                            valBytesIndex = valBytesIndex + noHighLen;
                        }
                    }else{
                        // 不是高位，存放3个字节
                        if ((valBytesIndex + 3) > len){
                            break;
                        }
                        valBytesIndex = valBytesIndex + 3;
                    }
                }else{
                    //大于0，以一个字节存储
                    if ((valBytesIndex + 1) > len){
                        break;
                    }
                    valBytesIndex++;
                }
            }
            rel = new String(valBytes, 0, valBytesIndex);
        }
        return rel;

    }
    /**
     * 默认是utf-8编码，中文是3个字符
     * @param value 需要截断的数据
     * @param standard_len  字段标准长度
     * @param split_flag  如果数据大于标准长度，末尾加的截断标志字符
     * @return 截断后的字符
     */
    public static String jieduan(String value, int standard_len, String split_flag) throws UnsupportedEncodingException {
        //默认utf-8
        String charname = "utf-8";
        return jieduan(value,standard_len,split_flag,charname);
    }
    /**
     * 默认是utf-8编码，中文是3个字符
     * @param value 需要截断的数据
     * @param standard_len  字段标准长度
     * @param split_flag  如果数据大于标准长度，末尾加的截断标志字符 默认..
     * @param charname 数据编码
     * @return 截断后的字符
     */
    public static String jieduan(String value, int standard_len, String split_flag, String charname) throws UnsupportedEncodingException {
        String rel = null;
        if (value != null && standard_len > 0){
            if (split_flag == null){
                split_flag = "..";
            }
            byte[] value_bytes = null;
            int ch_len = 0;
            if (charname == null || "utf-8".equals(charname)){
                value_bytes = value.getBytes("utf-8");
                ch_len = 3;
            }
            //真实数据的长度
            int value_len = value_bytes.length;
            //截取长度
            int iLen = 0;
            if (value_len>standard_len){
                //如果数据长度>标准长度
                //截断标志字符的长度
                int split_flag_len = split_flag.getBytes().length;
                //真实数据最终的长度
                int rel_len = standard_len - split_flag_len;
                for (int i = 0; i < rel_len; i++){
                    if ((value_bytes[i] & 0xFF) > 0x80){
                        //判断该字符是否是负数，如果是，说明是中文，截取长度+ch_len
                        //utf-8编制中文的长度：3
                        int end_len = iLen + ch_len;
                        if (end_len > rel_len){
                            break;
                        }
                        iLen += ch_len;
                        i += (ch_len-1);
                    }else{
                        iLen += 1;
                    }
                }
                String tmp = new String(value_bytes, 0, iLen);
                rel = tmp.substring(0,tmp.length()-1) + split_flag;
            }else{
                rel = value;
            }
        }
        return rel;
    }
}
