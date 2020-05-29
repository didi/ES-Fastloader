package com.didichuxing.fastindex.server;


import com.alibaba.fastjson.JSONObject;
import com.didichuxing.fastindex.common.es.IndexConfig;
import lombok.Data;

@Data
public class IndexFastIndexInfo {
    private IndexConfig indexConfig;
    private long reducerNum;
    private String transformType;

    public JSONObject toJson() {
        JSONObject obj = new JSONObject();
        obj.put("reducerNum", reducerNum);
        obj.put("indexConfig", indexConfig.toJson());
        obj.put("transformType", transformType);
        return obj;
    }

    @Override
    public String toString( ){
        return toJson().toJSONString();
    }
}
