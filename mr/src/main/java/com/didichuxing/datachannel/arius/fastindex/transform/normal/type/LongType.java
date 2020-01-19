package com.didichuxing.datachannel.arius.fastindex.transform.normal.type;


import com.alibaba.fastjson.JSONArray;

import java.util.HashSet;
import java.util.Set;

public class LongType extends Type {
    public static final Set<String> matchESTypes = new HashSet<>();
    public static final Set<String> matchHiveTypes = new HashSet<>();
    static {
        matchESTypes.add("integer".toUpperCase());
        matchESTypes.add("long".toUpperCase());
        matchESTypes.add("short".toUpperCase());
        matchESTypes.add("byte".toUpperCase());

        matchHiveTypes.add("tinyint".toUpperCase());
        matchHiveTypes.add("smallint".toUpperCase());
        matchHiveTypes.add("int".toUpperCase());
        matchHiveTypes.add("bigint".toUpperCase());
    }

    private boolean toStr;
    private boolean nullToZero;
    public LongType(String name, boolean toStr, boolean nullToZero ) {
        super(name);

        this.toStr =  toStr;
        this.nullToZero = nullToZero;
    }


    @Override
    public Object tranform(Object value) {
        if(value==null) {
            if(nullToZero) {
                return 0;
            } else {
                return null;
            }
        }

        try {
            Double.parseDouble(value.toString());

            if(toStr) {
                return value.toString();
            } else {
                return Long.valueOf(value.toString());
            }
        } catch (NumberFormatException e) {
            // 判断是否是boolean类型，如果是，则true转化成1， false转成0
            try {
                Boolean b = Boolean.parseBoolean(value.toString());
                if(b!=null) {
                    return b ? 1l : 0l;
                }

            } catch (Throwable t) {
               // nothing
            }

            // 判断是否是double类型
            try {
                Double d = Double.parseDouble(value.toString());
                return d;
            } catch (Throwable t) {
                // nothing
            }

            JSONArray array = toArray(value.toString());
            if (array != null) {
                return array;
            } else {
                tranformError(value.toString(), e);
                if (nullToZero) {
                    return 0;
                } else {
                    return null;
                }
            }
        }
    }
}
