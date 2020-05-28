package com.didichuxing.datachannel.arius.fastindex.mr.transform.normal.type;

import com.alibaba.fastjson.JSONArray;

import java.util.HashSet;
import java.util.Set;

public class BooleanType extends Type {
    public static final Set<String> matchESTypes = new HashSet<>();
    public static final Set<String> matchHiveTypes = new HashSet<>();
    static {
        matchESTypes.add("boolean".toUpperCase());

        matchHiveTypes.add("boolean".toUpperCase());
    }

    public BooleanType(String name) {
        super(name);
    }

    @Override
    public Object tranform(Object value) {
        if(value==null) {
            // 默认为false
            return null;
        }

        try {
            return Boolean.parseBoolean(value.toString());
        } catch (NumberFormatException e) {
            JSONArray array = toArray(value.toString());
            if (array != null) {
                return array;
            } else {
                tranformError(value.toString(), e);
                return null;
            }
        }
    }
}
