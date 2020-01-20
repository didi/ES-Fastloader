package com.didichuxing.datachannel.arius.fastindex.transform;

import com.alibaba.fastjson.JSONObject;
import org.apache.hive.hcatalog.common.HCatException;

import java.util.List;

/* 负责将Hive数据转化成写入ES的Json数据 */
public interface Transformer {
    public JSONObject tranform(List<Object> valueList) throws HCatException;
}
