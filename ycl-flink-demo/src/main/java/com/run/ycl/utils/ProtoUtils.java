package com.run.ycl.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class ProtoUtils {
    private static final Logger log = LoggerFactory.getLogger(ProtoUtils.class);

    /**
     * 字符集: UTF-8
     */
    public static final Charset UTF8 = Charset.forName("UTF-8");


    public final static String getClassNameByDataKey(String dataKey) {
        String[] arr = dataKey.split(",");
        String namespace = arr[0].trim();
        String aliasDSID = arr[1].trim();
        String className = getClassName(namespace, aliasDSID);
        return className;
    }

    public final static String getClassName(String namespace, String aliasDSID) {
        // 模式1:每个命名空间下的每个数据集单独一个proto文件.
        String className = namespace + "." + aliasDSID + "_$" + aliasDSID;

        // 模式2:每个命名空间下的所有数据集共用一个proto文件.
        // String className = namespace + ".DataSet$" + aliasDSID;

        // 模式3:不区分namespace
        // String className;
        // if ("XFILE".equals(namespace)) {
        // className = namespace + "." + aliasDSID + "_$" + aliasDSID;
        // } else {
        // className = "com.run.ayena.dataset." + aliasDSID + "_$" + aliasDSID;
        // }
        return className;
    }

    public final static Message.Builder getMessageBuilderByDataKey(
            String dataKey) {
        return getMessageBuilderByName(getClassNameByDataKey(dataKey));
    }

    public final static Message.Builder getMessageBuilderByName(String className) {
        try {
            Class<?> clazz = Class.forName(className + "$Builder");
            Method method = clazz.getDeclaredMethod("create");
            method.setAccessible(true);
            Message.Builder builder = (Message.Builder) method.invoke(null);
            log.info("getMessageBuilderByName ok: " + className);
            return builder;
        } catch (Exception e) {
            log.error("getMessageBuilderByName failed: " + className, e);
            return null;
        }
    }

    public final static Parser<? extends Message> getParserByDataKey(
            String dataKey) {
        String className = getClassNameByDataKey(dataKey);
        try {
            Class<?> clazz = Class.forName(className);
            Field field = clazz.getDeclaredField("PARSER");
            @SuppressWarnings("unchecked")
            Parser<? extends Message> parser = (Parser<? extends Message>) field
                    .get(null);
            return parser;
        } catch (Exception e) {
            log.error(
                    "getParserByDataKey: " + dataKey + ", class=" + className,
                    e);
        }
        return null;
    }

    /**
     * 解析Datakey值
     * 
     * @param value
     *            原始值
     * @return
     */
    public final static String getDataKey(BytesWritable key) {
        // 支持KEY组成: namespace+','+dataset[+','+rowkey]
        String keyStr = new String(key.getBytes(), 0, key.getLength(), UTF8);
        String[] arr = keyStr.split(",");
        String datakey = arr[0] + "," + arr[1];
        return datakey;
    }

    /**
     * 解析Datakey值
     * 
     * @param value
     *            原始值
     * @return
     */
    public final static String getDataKey(String keyStr) {
        // 支持KEY组成: namespace+','+dataset[+','+rowkey]
        String[] arr = keyStr.split(",");
        String datakey = arr[0] + "," + arr[1];
        return datakey;
    }

}
