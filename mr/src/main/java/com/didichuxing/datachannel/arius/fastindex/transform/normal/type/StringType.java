package com.didichuxing.datachannel.arius.fastindex.transform.normal.type;

import com.alibaba.fastjson.JSONArray;

import java.util.HashSet;
import java.util.Set;

public class StringType extends Type {
    public static final Set<String> matchESTypes = new HashSet<>();
    public static final Set<String> matchHiveTypes = new HashSet<>();
    static {
        matchESTypes.add("string".toUpperCase());
        matchESTypes.add("text".toUpperCase());
        matchESTypes.add("keyword".toUpperCase());
        matchESTypes.add("date".toUpperCase());

        matchHiveTypes.add("string".toUpperCase());
        matchHiveTypes.add("char".toUpperCase());
        matchHiveTypes.add("varchar".toUpperCase());
        matchHiveTypes.add("binary".toUpperCase());
        matchHiveTypes.add("date".toUpperCase());
        matchHiveTypes.add("timestamp".toUpperCase());
    }


    public StringType(String name) {
        super(name);
    }

    @Override
    public Object tranform(Object value) {
        if(value==null) {
            return null;
        }

        return value.toString();
    }
}
