package com.didichuxing.fastindex.server;


import com.alibaba.fastjson.JSONObject;
import com.didichuxing.fastindex.common.es.IndexConfig;
import lombok.Data;

@Data
public class IndexFastIndexInfo {
    private IndexConfig indexConfig;
    private long shardNum;
    private String transformType;
    private long expanfactor;
    private boolean smallData;
    private String intFilterKeys;

    private boolean longToStr = false;
    private boolean longNullToZero = false;
    private boolean removeBracket = false;
    private boolean isPassengerPre = false;
    private boolean null2Null = false;
    private String shardField = null;
    private String strToArray = null;

    public JSONObject toJson() {
        JSONObject obj = new JSONObject();
        obj.put("shardNum", shardNum);
        obj.put("indexConfig", indexConfig.toJson());
        obj.put("transformType", transformType);
        obj.put("expanfactor", expanfactor);
        obj.put("smallData", smallData);
        obj.put("intFilterKeys", intFilterKeys);
        obj.put("longToStr", longToStr);
        obj.put("longNullToZero", longNullToZero);
        obj.put("removeBracket", removeBracket);
        obj.put("isPassengerPre", isPassengerPre);
        obj.put("null2Null", null2Null);
        obj.put("shardField", shardField);
        obj.put("strToArray", strToArray);
        return obj;
    }

    @Override
    public String toString( ){
        return toJson().toJSONString();
    }
}
