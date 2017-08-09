package com.tyaer.hadoop.mr.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;

import java.io.UnsupportedEncodingException;

/**
 * Created by Twin on 2017/2/27.
 */
public class MapUtils {
    public static final byte[] FAMILY = "fn".getBytes();

    public static String getValue(Result result, String name) {
        String rs = "";
        byte[] bytes = name.getBytes();
        byte[] value = result.getValue(FAMILY, bytes);
        if (value != null) {
            try {
                rs = new String(value, "utf-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return rs;
    }

    public static Integer getInteger(String str) {
        Integer integer = 0;
        if (StringUtils.isNotBlank(str)) {
            str = str.trim();
            try {
                integer = Integer.valueOf(str);
            } catch (Exception e) {
                System.out.println("string getInteger fail:" + str);
            }
        }
        return integer;
    }
}
