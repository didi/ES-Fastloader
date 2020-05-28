package com.didichuxing.datachannel.arius.fastindex.mr.transform.normal.type;


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

    public LongType(String name) {
        super(name);
    }


    @Override
    public Object tranform(Object value) {
        if (value == null) {
            return null;
        }

        try {
            return Long.valueOf(value.toString());
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
