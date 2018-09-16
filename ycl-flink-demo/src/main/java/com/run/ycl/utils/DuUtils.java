package com.run.ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Map;

/**
 * 归一化工具类
 *
 * 功能函数
 */
public class DuUtils {


    /**
     * 根据函数进行归一化操作
     *
     * @param valueJson 数据json
     * @param dmField   策略json
     * @param function  归一化函数名
     */
    public static void all_normalizing(JSONObject valueJson, JSONObject dmField, String function) {

        //策略不为空
        if(valueJson!=null && dmField!=null && function!=null){

            //目的编码
            String duElementKey = dmField.getString("Element");

            //条件
            JSONObject conditionsJson = dmField.getJSONObject("Conditions");

            //进行条件判断
            boolean expBool =true;
         //   boolean expBool = DmUtils.validateExpression(valueJson, conditionsJson);

            if (expBool) {
                JSONObject expressionJson = dmField.getJSONObject("Expression");

                JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");

                Map<String, String> paramMap = JsonUtils.convertJsonArrayToMap(paramJsonArray, "Name", "Value");

                if (paramMap.containsKey("element")){
                    String elementValue = paramMap.get("element");
                    String value = valueJson.getString(elementValue);

                    String normaliz_value = null;
                    //进行归一化操作
                    switch (function){

                        case "du_card":
                            normaliz_value = duCard(value);
                            break;
                        case "du_normal" :
                            normaliz_value = duNormal(value);
                            break;
                        case "du_mac":
                            normaliz_value = duMac(value);
                            break;
                        case "du_mobile":
                            normaliz_value = duMobile(value);
                            break;
                        case "du_url":
                            normaliz_value = duUrl(value);
                            break;
                        case "du_imei":
                            normaliz_value = duImei(value);
                            break;
                        case "du_imsi":
                            normaliz_value = duImsi(value);
                            break;
                        default:
                            break;
                    }

                    if (normaliz_value!=null){
                        valueJson.put(duElementKey,normaliz_value);
                    }
                }



            }
        }
    }




    /**
     <NormalizedField Element="500030B">
        <Expression Function="du_card">
            <Param Name="element" Value="B030005"/>
        </Expression>
        <Conditions Rel="OR">
            <Condition Key="B030004" Value="111" Opr="Equal" Fmt=""/>
        </Conditions>
     </NormalizedField>


     <NormalizedField Element="500030B">
        <Expression Function="du_card">
            <Param Name="element" Value="B030005"/>
        </Expression>
        <Conditions Rel="OR">
            <Condition Key="B030004" Value="335" Opr="Equal" Fmt=""/>
        </Conditions>
     </NormalizedField>
     * @param value 身份证号
     * @return 处理后的数据
     */
    public static String duCard(String value) {


        // 对身份证号进行归一化处理，统一将身份证号码去掉出生日期的两位世纪和最后的校验值后，
        // 全部转换成15位半角数字表示。
        //144564 1995 0811 549x
        //144564 95 0811 549

        String normaliz_value = null;


        if(value!=null){

            int valueLen = value.length();
            if (valueLen == 15){
                //如果身份证是15位，直接返回

                normaliz_value = value;

            }else if (valueLen == 18){

                //如果是18位，进行截取操作，然后返回
                String normaliz_value_1 = value.substring(0,6);
                String normaliz_value_2 = value.substring(8,valueLen-1);
                normaliz_value = normaliz_value_1+normaliz_value_2;

            }

        }

        return normaliz_value;

    }




    /**
     * <NormalizedField Element="500030B">
            <Expression Function="du_normal">
                <Param Name="element" Value="B030005"/>
            </Expression>
     </NormalizedField>
     * @param value 特定字段数据
     * @return 转换后的数据
     */
    public static String duNormal(String value) {



        //所有需要归一化的特定字段进行全角转半角，大写转小写的归一化操作。


        String normaliz_value = null;


        if(value!=null){

            //进行全角转半角操作
            String banjiao = quan_ban(value);

            if (banjiao!=null){

                normaliz_value = banjiao.toLowerCase();

            }
        }
        return normaliz_value;

    }

    /**
     * 进行半角转全角操作
     * @param src 字符
     * @return 转换为全角
     */
    private static String ban_quan(String src){
//        String src="■ ■ｆｆａｃ●●△※○○☆№ccc ａｂｃ１２３ｈａｈａnhao";
       String rel = null;
        if (src!=null){
            char c[] = src.toCharArray();
            for (int i = 0; i < c.length; i++) {
                if (c[i] == ' ') {
                    c[i] = '\u3000';
                } else if (c[i] < '\177') {
                    c[i] = (char) (c[i] + 65248);

                }
            }
            rel = new String (c);
        }
        return rel;

    }

    /**
     * 进行全角转半角操作
     * @param src 字符
     * @return 转换为半角
     */
    private static String quan_ban(String src){
//        String src="■ ■ｆｆａｃ●●△※○○☆№ccc ａｂｃ１２３ｈａｈａnhao";
        String rel = null;
        if (src!=null){
            char[] c = src.toCharArray();
            for (int index = 0; index < c.length; index++) {
                if (c[index] == 12288) {// 全角空格
                    c[index] = (char) 32;
                } else if (c[index] > 65280 && c[index] < 65375) {// 其他全角字符
                    c[index] = (char) (c[index] - 65248);
                }
            }
            rel = new String (c);
        }

        return rel;

    }
    /**
     <NormalizedField Element="200040C">
        <Expression Function="du_mac">
            <Param Name="element" Value="C040002"/>
        </Expression>
     </NormalizedField>
     * @param value Mac地址
     * @return 处理后的数据
     */
    public static String duMac(String value) {

        //统一将MAC地址全部采用字母小写并去掉所有分隔字符，仅存储12位小写字母和半角数字的组合。

        String normaliz_value = null;


        if(value!=null){

            if (value.length() == 17){
                //先转为半角
                String normaliz_value_1 = quan_ban(value);

                //转为小写
                if (normaliz_value_1!=null){
                    normaliz_value_1 = normaliz_value_1.toLowerCase();
                    //去掉分隔符
                    normaliz_value_1 = normaliz_value_1.replaceAll("-","");
                    if (normaliz_value_1.length() == 12){
                        normaliz_value = normaliz_value_1;
                    }
                }
            }

        }

        return normaliz_value;

    }

    /**
     <NormalizedField Element="500020B">
        <Expression Function="du_mobile">
            <Param Name="element" Value="B020005"/>
        </Expression>
     </NormalizedField>
     * @param value 手机号
     * @return 返回处理后的手机号
     */
    public static String duMobile(String value) {


        //统一将手机号码转换, 从后向前保留11位半角数字（对于境外手机，不超过11位，则保留原号码）

        String normaliz_value = null;


        if(value!=null){

            //进行全角转半角
            String normaliz_value_1 = quan_ban(value);

            if (normaliz_value_1!=null){


                //判断长度
                int len = normaliz_value_1.length();

                if (len > 11){

                    normaliz_value = normaliz_value_1.substring(len-11,len);
                }else {

                    normaliz_value = normaliz_value_1;
                }

            }

        }
        return normaliz_value;

    }

    /**
     <NormalizedField Element="200010G">
        <Expression Function="du_url">
            <Param Name="element" Value="G010002"/>
        </Expression>
     </NormalizedField>
     * @param  value 数据
     * @return 返回处理后的url
     */
    public static String duUrl(String value) {


        //统一将URL地址全角转半角，小写字母，
        // URL地址统一删除"http://"或"https://"
        // （针对URL中www做特殊处理，将“www.”同时删除，
        // 即www.baidu.com和baidu.com视为同一个URL；www1.baidu.com和baidu.com视为不同的URL）


        String normaliz_value = null;


        if(value!=null){

            //进行全角转半角
            String normaliz_value_1 = quan_ban(value);

            if(normaliz_value_1!=null){

                //进行转小写
                normaliz_value_1 = normaliz_value_1.toLowerCase();

                //删除"http://"或"https://"
                if (normaliz_value_1.startsWith("http://") || normaliz_value_1.startsWith("https://")){
                    normaliz_value_1 = normaliz_value_1.replaceFirst("http://","").replaceFirst("https://","");
                }


                if (normaliz_value_1.startsWith("www.")){

                    normaliz_value_1 = normaliz_value_1.replaceFirst("www.","");

                }

                normaliz_value = normaliz_value_1;

            }

        }

        return normaliz_value;

    }

    /**
     *
     * @param value imsi数据
     * @return 处理后的数据
     */
    private static String duImsi(String value) {

        String normaliz_value = null;
        if (value!=null){

            int valueLen = value.length();
            if (valueLen == 15 || valueLen == 17){
                //简单判断位数
                //全角转半角

                normaliz_value = quan_ban(value);

            }

        }
        return normaliz_value;
    }


    /**
     *
     * @param value imei格式的数据
     * @return 处理后的数据
     */
    private static String duImei(String value) {


        String normaliz_value = null;
        if (value!=null){

            int valueLen = value.length();
            if (valueLen == 15){
                //简单判断位数
                //全角转半角

                normaliz_value = quan_ban(value);

            }

        }
        return normaliz_value;
    }
}
