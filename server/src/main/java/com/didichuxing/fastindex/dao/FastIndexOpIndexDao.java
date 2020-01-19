package com.didichuxing.fastindex.dao;

import com.alibaba.fastjson.JSON;
import com.didichuxing.fastindex.common.po.FastIndexOpIndexPo;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class FastIndexOpIndexDao extends BaseEsDao {
    private static final String INDEX_NAME = "fast.index.op";

    public boolean batchInsert(List<FastIndexOpIndexPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }


    public List<FastIndexOpIndexPo> getUnFinished() {
        String dslFormat = "{\n" +
                "  \"size\": 10000,\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"finish\": {\n" +
                "        \"value\": \"false\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String source = query(INDEX_NAME, dslFormat);
        return JSON.parseArray(source, FastIndexOpIndexPo.class);

    }

    public void delete(String key) {
        delete(INDEX_NAME, TYPE_NAME, key);
    }

    public List<FastIndexOpIndexPo> getFinishedByIndexName(String indexName) {
        String dslFormat = "{\n" +
                "  \"size\": 10000,\n" +
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
                "            \"finish\": {\n" +
                "              \"value\": \"true\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String dsl = String.format(dslFormat, indexName);
        String source = query(INDEX_NAME, dsl);
        return JSON.parseArray(source, FastIndexOpIndexPo.class);
    }
}
