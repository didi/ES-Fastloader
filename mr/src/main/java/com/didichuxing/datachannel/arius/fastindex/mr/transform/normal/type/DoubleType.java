package com.didichuxing.datachannel.arius.fastindex.mr.transform.normal.type;

import com.alibaba.fastjson.JSONArray;

import java.util.HashSet;
import java.util.Set;

public class DoubleType extends Type {
    public static final Set<String> matchESTypes = new HashSet<>();
    public static final Set<String> matchHiveTypes = new HashSet<>();
    static {
        matchESTypes.add("double".toUpperCase());
        matchESTypes.add("float".toUpperCase());
        matchESTypes.add("half_float".toUpperCase());
        matchESTypes.add("scaled_float".toUpperCase());

        matchHiveTypes.add("float".toUpperCase());
        matchHiveTypes.add("double".toUpperCase());
        matchHiveTypes.add("decimal".toUpperCase());
    }

    public DoubleType(String name) {
        super(name);
    }

    @Override
    public Object tranform(Object value) {
        if(value==null) {
            return null;
        }

        try {
            return Double.parseDouble(value.toString());
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
