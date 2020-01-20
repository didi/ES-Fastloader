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

    public JSONObject toJson() {
        JSONObject obj = new JSONObject();
        obj.put("shardNum", shardNum);
        obj.put("indexConfig", indexConfig.toJson());
        obj.put("transformType", transformType);
        obj.put("expanfactor", expanfactor);
        return obj;
    }

    @Override
    public String toString( ){
        return toJson().toJSONString();
    }
}
