package com.run.ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 *
 * 数据校验工具类
 *
 * 功能函数
 *
 *
 */
public class DvUtils {
    private static final Logger logger = LoggerFactory.getLogger(DvUtils.class);
    private static Map<String,Pattern> patternMap = new HashMap<>();
    static {
        init();
    }
    private static void init(){

        InputStream inputStream = DvUtils.class.getClassLoader().getResourceAsStream("reg.properties");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        Properties p = new Properties();
        try{
            p.load(inputStream);
        }catch (IOException e){
            e.printStackTrace();
        }
        for(Object key : p.keySet()){
            patternMap.put((String)key,Pattern.compile(p.getProperty((String)key)));
        }
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    private static String getFunctionType(JSONObject dvField) {
        String functionType="";
        if(dvField != null) {
            JSONObject expressionJson = dvField.getJSONObject("Expression");
            functionType=expressionJson.getString("Function");
        }
        return functionType;
    }

    private static String getSpecificField(JSONObject dvField,String name) {
        if(dvField != null && StringUtils.isNotBlank(name)) {
            //String dmElementKey = dvField.getString("Element");
            JSONObject expressionJson = dvField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");

            Map<String, String> paramMap = JsonUtils.convertJsonArrayToMap(paramJsonArray, "Name", "Value");
            if(null != paramMap)
                return paramMap.get(name);
        }
        return "";
    }
    /**
     * 必填校验
     *
     * <NormalizedField Element="I010005">
     *     <Expression Function="dv_must">
     *         <Param Name="element" Value="I010005"/>
     *     </Expression>
     * </NormalizedField>
     * 必填项校验规则
     *
     * @param valueJson
     * @param dmField
     */
    public static boolean dvMust(JSONObject valueJson, JSONObject dmField) {
        boolean valadateBool = true;
        if (dmField != null && valueJson != null) {
            String elementField = getSpecificField(dmField, "element");
            String eValue = valueJson.getString(elementField);
            valadateBool = StringUtils.isNotBlank(eValue);
            if (!valadateBool) {
                if (logger.isDebugEnabled()) {
                    logger.debug("validate error rule:{},field code:{}, field value:{}, data:{}",new String[]{"dv_must", elementField, eValue, valueJson.toString()});
                }
            }
        }
        return valadateBool;
    }


    /**
     * 必填校验
     *
     * <NormalizedField Element="Z002244">
     *     <Expression Function="dv_mac">
     *         <Param Name="element" Value="Z002244"/>
     *     </Expression>
     * </NormalizedField>
     *
     * mac地址校验规则，支持格式：xx-xx-xx-xx-xx-xx 或者 xx：xx:xx:xx:xx:xx
     *
     * @param valueJson
     * @param dvField
     */
    public static boolean RegValidate(JSONObject valueJson, JSONObject dvField) {
        boolean valadateBool = true;
        if (dvField != null && valueJson != null) {
            String functionType = getFunctionType(dvField);
            Pattern pattern = patternMap.get(functionType);
            String field = getSpecificField(dvField, "element");
            if(pattern != null){
                String str = valueJson.getString(field);
                if(str != null && !str.isEmpty())
                    valadateBool = pattern.matcher(str).matches();
                if(!valadateBool){
                    if (logger.isDebugEnabled()) {
                        logger.debug("validate error rule:{},field code:{}, field value:{}, data:{}",new String[]{functionType, field, str, valueJson.toString()});
                    }
                }
            }
        }

        return valadateBool;
    }

    public static void main(String[] args) {
        for(Map.Entry<String,Pattern> entry: DvUtils.patternMap.entrySet()){
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println(DvUtils.patternMap);
        System.out.println( DvUtils.patternMap.get("dv_card").matcher("130424198504054073").matches());
        System.out.println( DvUtils.patternMap.get("dv_card").matcher("130424199504054073").matches());
        System.out.println( DvUtils.patternMap.get("dv_ip").matcher("192.168.1.1").matches());
        System.out.println( DvUtils.patternMap.get("dv_lat").matcher("56.8934").matches());
        System.out.println( DvUtils.patternMap.get("dv_lon").matcher("135.555").matches());
        System.out.println( DvUtils.patternMap.get("dv_mac").matcher("BC-AE-C5-75-EC-1A").matches());
        System.out.println( DvUtils.patternMap.get("dv_mobile").matcher("18602370235").matches());
        System.out.println( DvUtils.patternMap.get("dv_number").matcher("56").matches());
        System.out.println( DvUtils.patternMap.get("dv_email").matcher("abc@qq.com").matches());


    }
}
