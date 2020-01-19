package com.didichuxing.fastindex.common.po;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexTaskMetricPo extends BasePo {

    private String srcTag;

    private String indexName;

    private long shardNum;

    private long addTime;

    private JSONObject metrics;

    @Override
    public String getKey() {
        if(srcTag==null || srcTag.trim().length()==0) {
            return indexName + "_" + shardNum;
        } else {
            return srcTag.trim() + "_" + indexName + "_" + shardNum;
        }
    }
}
