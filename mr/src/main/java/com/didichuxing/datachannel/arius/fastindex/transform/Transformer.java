package com.didichuxing.datachannel.arius.fastindex.transform;

import com.alibaba.fastjson.JSONObject;
import org.apache.hive.hcatalog.common.HCatException;

import java.util.List;

public interface Transformer {
    public JSONObject tranform(List<Object> valueList) throws HCatException;
}
