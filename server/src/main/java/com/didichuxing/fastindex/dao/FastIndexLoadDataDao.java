package com.didichuxing.fastindex.dao;

import com.alibaba.fastjson.JSON;
import com.didichuxing.fastindex.common.po.FastIndexLoadDataPo;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FastIndexLoadDataDao extends BaseEsDao {
    /* 索引功能说明 */
    private static final String INDEX_NAME = "fast.index.loadata";

    public boolean batchInsert(List<FastIndexLoadDataPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    public List<FastIndexLoadDataPo> getAll() {
        String dsl = "{" +
                "   \"size\": 10000\n" +
                " }";

        String source = query(INDEX_NAME, dsl);
        return JSON.parseArray(source, FastIndexLoadDataPo.class);
    }
}
