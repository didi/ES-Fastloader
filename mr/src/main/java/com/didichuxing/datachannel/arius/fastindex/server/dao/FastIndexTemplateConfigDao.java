package com.didichuxing.datachannel.arius.fastindex.server.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.didichuxing.datachannel.arius.fastindex.server.common.po.FastIndexTemplateConfigPo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FastIndexTemplateConfigDao extends BaseEsDao {
    private static final String INDEX_NAME = "fast.index.config";

    public static boolean batchInsert(List<FastIndexTemplateConfigPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    public static FastIndexTemplateConfigPo getByName(String templateName) {
        String dslFormat = "{\n" +
                "  \"size\": 10,\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"name\": {\n" +
                "        \"value\": \"%s\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String dsl = String.format(dslFormat, templateName);
        String source = query(INDEX_NAME, dsl);
        return JSON.parseObject(source, FastIndexTemplateConfigPo.class);
    }

    public static List<FastIndexTemplateConfigPo> getAll() {
        String dsl = "{\n" +
                "  \"size\": 10000\n" +
                "}";
        String source = query(INDEX_NAME, dsl);
        return JSONArray.parseArray(source, FastIndexTemplateConfigPo.class);
    }

    public static Map<String, FastIndexTemplateConfigPo> getAllByMap() {
        List<FastIndexTemplateConfigPo> l = getAll();

        Map<String, FastIndexTemplateConfigPo> ret = new HashMap<String, FastIndexTemplateConfigPo>();
        for(FastIndexTemplateConfigPo fastIndexTemplateConfigPo : l) {
            ret.put(fastIndexTemplateConfigPo.getName(), fastIndexTemplateConfigPo);
        }

        return ret;
    }

}
