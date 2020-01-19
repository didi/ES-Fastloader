package com.didichuxing.datachannel.arius.fastindex.transform.normal.type;

import com.alibaba.fastjson.JSON;

import java.util.HashSet;
import java.util.Set;

// 这里的类型判断保存在远端，方便升级
public class JsonType extends Type {
    public static final Set<String> matchESTypes = new HashSet<>();
    public static final Set<String> matchHiveTypes = new HashSet<>();
    static {
        matchESTypes.add("object".toUpperCase());
        matchESTypes.add("nested".toUpperCase());
        matchESTypes.add("geo_point".toUpperCase());
        matchESTypes.add("geo_shape".toUpperCase());
    }

    private boolean null2Null;

    public JsonType(String name, boolean null2Null) {
        super(name);
        this.null2Null = null2Null;
    }

    @Override
    public Object tranform(Object value) {
        if(value==null) {
            if(null2Null) {
                return null;
            } else {
                return "";
            }
        }

        try {
            return JSON.parse(value.toString());
        } catch (Throwable t) {
            // 解析json失败
            tranformError(value.toString(), t);
            return value.toString();
        }
    }
}
