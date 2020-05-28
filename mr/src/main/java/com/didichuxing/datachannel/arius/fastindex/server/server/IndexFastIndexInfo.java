package com.didichuxing.datachannel.arius.fastindex.server.server;


import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.server.common.es.IndexConfig;
import lombok.Data;

@Data
public class IndexFastIndexInfo {
    private IndexConfig indexConfig;
    private long reduceNum;
    private String transformType;
    private long expanfactor;

    public JSONObject toJson() {
        JSONObject obj = new JSONObject();
        obj.put("reduceNum", reduceNum);
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
