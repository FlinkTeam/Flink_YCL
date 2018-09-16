package com.run.ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;

public class ConvertUtils {

    //采用本地缓存进行优化
    public static JSONObject convertString2Json(String value, String template) throws Exception {
        JSONObject jsonObject = new JSONObject();
        if (StringUtils.isNotBlank(value)) {
            String[] vlueList = value.split("\\t", 65535);
            JSONObject templateJson = JSONObject.parseObject(template);

            JSONArray dataSetArray = templateJson.getJSONArray("DATASET");
            Iterator<Object> dataSetIterator = dataSetArray.iterator();
            while (dataSetIterator.hasNext()) {
                JSONObject dataSetJson = (JSONObject) dataSetIterator.next();
                if (dataSetJson.getString("name").equals("WA_COMMON_010015")) {
                    JSONObject contentJson = dataSetJson.getJSONObject("DATA");
                    if (contentJson.getJSONArray("ITEM").size() == vlueList.length) {
                        Iterator<Object> itemItertor = contentJson.getJSONArray("ITEM").iterator();
                        int i = 0;
                        while (itemItertor.hasNext()) {
                            JSONObject itemJson = (JSONObject) itemItertor.next();
                            jsonObject.put(itemJson.getString("key"), vlueList[i++]);
                        };
                    } else {
                        System.out.println("===========convertString2Json error========" );
                        System.out.println("value:" + value + "---value.size:" + vlueList.length);
                        System.out.println("template:" + template + "---template.size:" + contentJson.getJSONArray("ITEM").size());
                        System.out.println("===========convertString2Json error========" );
                    }

                }
            }

        } else {
            throw new Exception("对象转换异常！");
        }
        return jsonObject;
    }

    public static byte[] jsonToProtoBtyeArray(String protocolName, JSONObject json) {
        if (json == null || StringUtils.isBlank(protocolName)) {
            return null;
        }
        try {
            //Class<?> clazz = Class.forName("com.run.ycl.protobean." + protocolName +"_PROTO$PB_"+protocolName+"$Builder");
            Class<?> clazz = Class.forName("com.run.ycl.protobean." + protocolName +"_PROTO$PB_"+protocolName);
            Method method = clazz.getDeclaredMethod("newBuilder");
            method.setAccessible(true);

            Message.Builder builder = (Message.Builder) method.invoke(null, null);
            Descriptors.Descriptor descriptor = builder.getDescriptorForType();

            Iterator<Map.Entry<String, Object>> it = json.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> field = it.next();
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getKey());
                if (fieldDescriptor == null) {
                    //log .debug
                } else {
                    if (!"array".equals("array")) {
                        //如果是数组
                        /**
                         * 需要做什么样的单独处理，有待进一步研究；
                         */
                        //builder.setField(fieldDescriptor, "");
                    } else {
                        //如果是普通类型
                        builder.setField(fieldDescriptor, field.getValue());
                    }
                }
            }

            //System.out.println(JsonFormat.printer().print(builder.build()).toString());
            ByteArrayOutputStream byteArrayInputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectInputStream = new ObjectOutputStream(byteArrayInputStream);
            objectInputStream.writeObject(builder.build());
            return byteArrayInputStream.toByteArray();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
