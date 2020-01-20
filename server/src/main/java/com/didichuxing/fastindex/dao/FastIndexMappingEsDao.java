package com.didichuxing.fastindex.dao;

import com.alibaba.fastjson.JSON;
import com.didichuxing.fastindex.common.po.FastIndexMappingPo;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class FastIndexMappingEsDao extends BaseEsDao {
    private static final String INDEX_NAME = "fast.index.mapping";

    public boolean batchInsert(List<FastIndexMappingPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    public List<FastIndexMappingPo> getByindexName(String processIndex, int offset) {
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
        return JSON.parseArray(source, FastIndexMappingPo.class);

    }

    public List<FastIndexMappingPo> getByindexNameAndSrcTag(String srcTag, String processIndex, int offset) {
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
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"indexName\": {\n" +
                "              \"value\": \"%s\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"srcTag\": {\n" +
                "              \"value\": \"%s\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String dsl = String.format(dslFormat, offset, processIndex, srcTag);
        String source = query(INDEX_NAME, dsl);
        return JSON.parseArray(source, FastIndexMappingPo.class);

    }

    public List<FastIndexMappingPo> getByindexName(String srcTag, String processIndex) {
        List<FastIndexMappingPo> ret = new ArrayList<FastIndexMappingPo>();
        while(true) {
            List<FastIndexMappingPo> l = null;

            if(srcTag==null || srcTag.trim().length()==0) {
                l = getByindexName(processIndex,ret.size());
            } else {
                l = getByindexNameAndSrcTag(srcTag, processIndex, ret.size());
            }
            if(l==null || l.size()==0) {
                return ret;
            }

            ret.addAll(l);
        }
    }
}
