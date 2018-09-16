package com.run.ycl.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ProtoBufUtil {

    private static final Logger logger = LoggerFactory.getLogger(ProtoBufUtil.class);

    public final static Message.Builder getMessageBuilderByName(String className) {
        try {
            Class<?> clazz = Class.forName(className + "$Builder");
            Method method = clazz.getDeclaredMethod("create");
            method.setAccessible(true);
            Message.Builder builder = (Message.Builder) method.invoke(null);
            logger.info("getMessageBuilderByName ok: " + className);
            return builder;
        } catch (Exception e) {
            logger.error("getMessageBuilderByName failed: " + className, e);
            return null;
        }
    }

    public static byte[] jsonToProtoBtyeArray(String protocolName, String jsonStr) {
        if (jsonStr == null || StringUtils.isBlank(protocolName)) {
            return null;
        }

        Class<?> clazz = null;
        try {

            String className = "com.run.ayena.dataset." + protocolName +"_$" + protocolName;
            clazz = Class.forName(className);
            if (logger.isDebugEnabled()) {
                logger.debug("className:{}",  className);
            }

            Method method = clazz.getDeclaredMethod("newBuilder");
            method.setAccessible(true);

            Message.Builder builder = (Message.Builder) method.invoke(null, null);

            JsonFormat jsonFormat = new JsonFormat();
            jsonFormat.merge(jsonStr, ExtensionRegistry.getEmptyRegistry(), builder);

            if (logger.isDebugEnabled()) {
                logger.debug("json To Protobuf: {}", builder.build().toString());
            }

            return builder.build().toByteArray();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (JsonFormat.ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args) {
        JSONObject json = new JSONObject();
        json.put("RZ002000", "1111");
        json.put("RZ002001", "2222");
        json.put("RG020032", "333");

        // RWA_BASIC_0001_.RWA_BASIC_0001
        String protoName = "WA_BASIC_0001";
        ProtoBufUtil.jsonToProtoBtyeArray(protoName, json.toJSONString());

       /* Message.Builder builder = getMessageBuilderByName("com.run.ayena.dataset.RWA_BASIC_0001_$RWA_BASIC_0001");
        System.out.println(builder.getAllFields().size());

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
                    *//**
                     * 需要做什么样的单独处理，有待进一步研究；
                     *//*
                    //builder.setField(fieldDescriptor, "");
                } else {
                    //如果是普通类型
                    builder.setField(fieldDescriptor, field.getValue());
                }
            }
        }

        System.out.println(builder.build().getAllFields().size());*/
    }
}
