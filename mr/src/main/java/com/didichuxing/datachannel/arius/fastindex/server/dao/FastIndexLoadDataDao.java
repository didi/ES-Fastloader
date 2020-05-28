package com.didichuxing.datachannel.arius.fastindex.server.dao;

import com.alibaba.fastjson.JSON;
import com.didichuxing.datachannel.arius.fastindex.server.common.po.FastIndexLoadDataPo;

import java.util.List;

public class FastIndexLoadDataDao extends BaseEsDao {
    private static final String INDEX_NAME = "fast.index.loadata";

    public static boolean batchInsert(List<FastIndexLoadDataPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    public static List<FastIndexLoadDataPo> getAll() {
        String dsl = "{" +
                "   \"size\": 10000\n" +
                " }";

        String source = query(INDEX_NAME, dsl);
        return JSON.parseArray(source, FastIndexLoadDataPo.class);
    }
}
