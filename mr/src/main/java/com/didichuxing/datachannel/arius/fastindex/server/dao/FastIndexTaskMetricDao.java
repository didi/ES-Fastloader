package com.didichuxing.datachannel.arius.fastindex.server.dao;

import com.alibaba.fastjson.JSON;
import com.didichuxing.datachannel.arius.fastindex.server.common.po.FastIndexTaskMetricPo;

import java.util.ArrayList;
import java.util.List;

public class FastIndexTaskMetricDao extends BaseEsDao {
    private static final String INDEX_NAME = "fast.index.metric";

    public static boolean batchInsert(List<FastIndexTaskMetricPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    private static List<FastIndexTaskMetricPo> getByindexName(String processIndex, int offset) {
        String dslFormat = "{\n" +
                "  \"sort\": [\n" +
                "    {\n" +
                "      \"reduceId\": {\n" +
                "        \"order\": \"asc\"\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"size\": 100,\n" +
                "  \"from\": %d,\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"indexName\": {\n" +
                "        \"value\": \"%s\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String dsl = String.format(dslFormat, offset, processIndex);
        String source = query(INDEX_NAME, dsl);
        return JSON.parseArray(source, FastIndexTaskMetricPo.class);
    }

    public static List<FastIndexTaskMetricPo> getByindexName(String processIndex) {
        List<FastIndexTaskMetricPo> ret = new ArrayList<FastIndexTaskMetricPo>();
        while(true) {
            List<FastIndexTaskMetricPo> l = getByindexName(processIndex,ret.size());
            if(l==null || l.size()==0) {
                return ret;
            }

            ret.addAll(l);
        }
    }
}
