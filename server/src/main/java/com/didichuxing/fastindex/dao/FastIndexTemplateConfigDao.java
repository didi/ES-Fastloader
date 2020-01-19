package com.didichuxing.fastindex.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.didichuxing.fastindex.common.po.FastIndexTemplateConfigPo;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class FastIndexTemplateConfigDao extends BaseEsDao {
    private static final String INDEX_NAME = "fast.index.config";

    public boolean batchInsert(List<FastIndexTemplateConfigPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    public FastIndexTemplateConfigPo getByName(String templateName) {
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

    public List<FastIndexTemplateConfigPo> getAll() {
        String dsl = "{\n" +
                "  \"size\": 10000\n" +
                "}";
        String source = query(INDEX_NAME, dsl);
        return JSONArray.parseArray(source, FastIndexTemplateConfigPo.class);
    }

    public Map<String, FastIndexTemplateConfigPo> getAllByMap() {
        List<FastIndexTemplateConfigPo> l = getAll();

        Map<String, FastIndexTemplateConfigPo> ret = new HashMap<String, FastIndexTemplateConfigPo>();
        for(FastIndexTemplateConfigPo fastIndexTemplateConfigPo : l) {
            ret.put(fastIndexTemplateConfigPo.getName(), fastIndexTemplateConfigPo);
        }

        return ret;
    }

}
