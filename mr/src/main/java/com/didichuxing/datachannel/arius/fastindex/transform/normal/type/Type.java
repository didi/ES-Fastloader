package com.didichuxing.datachannel.arius.fastindex.transform.normal.type;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;

public abstract class Type {
    protected String name;
    protected boolean isFirst = true;

    public Type(String name) {
        this.name = name;
    }

    public void addField(JSONObject data, Object value) {
        data.put(name, tranform(value));
    }

    public abstract Object tranform(Object value);

    protected JSONArray toArray(String str) {
        try {
            return JSON.parseArray(str);
        } catch (Throwable t) {
            return null;
        }
    }


    public static Type matchESType(String name, String type, IndexInfo indexInfo) {
        if(StringUtils.isBlank(type)) {
            return new StringType(name, indexInfo.isRemoveBracket(), indexInfo.isNull2Null(), indexInfo.getStrToArrayFields());
        }

        type = type.toUpperCase();

        if(BooleanType.matchESTypes.contains(type)) {
            return new BooleanType(name);
        }

        if(DoubleType.matchESTypes.contains(type)) {
            return new DoubleType(name, indexInfo.isLongNullToZero());
        }

        if(LongType.matchESTypes.contains(type)) {
            return new LongType(name, indexInfo.isLongToStr(), indexInfo.isLongNullToZero());
        }

        if(StringType.matchESTypes.contains(type)) {
            return new StringType(name, indexInfo.isRemoveBracket(), indexInfo.isNull2Null(), indexInfo.getStrToArrayFields());
        }

        if(JsonType.matchESTypes.contains(type)) {
            return new JsonType(name, indexInfo.isNull2Null());
        }

        return new StringType(name, indexInfo.isRemoveBracket(), indexInfo.isNull2Null(), indexInfo.getStrToArrayFields());
    }

    public static Type matchHiveType(String name, String type, IndexInfo indexInfo) {
        if (StringUtils.isBlank(type)) {
            return new StringType(name, indexInfo.isRemoveBracket(), indexInfo.isNull2Null(), indexInfo.getStrToArrayFields());
        }

        type = type.toUpperCase();

        if (BooleanType.matchHiveTypes.contains(type)) {
            return new BooleanType(name);
        }

        if (DoubleType.matchHiveTypes.contains(type)) {
            return new DoubleType(name, indexInfo.isLongNullToZero());
        }

        if (LongType.matchHiveTypes.contains(type)) {
            return new LongType(name, indexInfo.isLongToStr(), indexInfo.isLongNullToZero());
        }

        if (StringType.matchHiveTypes.contains(type)) {
            return new StringType(name, indexInfo.isRemoveBracket(), indexInfo.isNull2Null(), indexInfo.getStrToArrayFields());
        }

        if (JsonType.matchHiveTypes.contains(type)) {
            return new JsonType(name, indexInfo.isNull2Null());
        }

        return new StringType(name, indexInfo.isRemoveBracket(), indexInfo.isNull2Null(), indexInfo.getStrToArrayFields());
    }

    protected void tranformError(String s, Throwable t) {
        if(isFirst) {
            isFirst = false;
            LogUtils.error("field tranform error, name:" + name + ", value:" + s, t);
        }
    }
}
