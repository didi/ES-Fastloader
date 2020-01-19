package com.didichuxing.fastindex.dao;

import com.alibaba.fastjson.JSON;
import com.didichuxing.fastindex.common.po.FastIndexTaskMetricPo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class FastIndexTaskMetricDao extends BaseEsDao {
    private static final String INDEX_NAME = "fast.index.metric";

    /**
     * 查询模板聚合数据的索引名称
     */
    @Value("${arius.fast.index.task.mertic.index.name}")
    private String            indexName;

    /**
     * type名称
     */
    private String            typeName     = "type";


    public boolean batchInsert(List<FastIndexTaskMetricPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    private List<FastIndexTaskMetricPo> getByindexName(String processIndex, int offset) {
        String dslFormat = "{\n" +
                "  \"sort\": [\n" +
                "    {\n" +
                "      \"shardNum\": {\n" +
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

    public List<FastIndexTaskMetricPo> getByindexName(String processIndex) {
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
