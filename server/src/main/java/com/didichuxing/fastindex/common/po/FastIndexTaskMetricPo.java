package com.didichuxing.fastindex.common.po;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/* 单个reduce任务的metric数据 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexTaskMetricPo extends BasePo {

    private String srcTag;

    private String indexName;   // 索引名
    private long reduceId;      // reduce编号
    private long addTime;       // 提交时间
    private JSONObject metrics; // metric数据

    @Override
    public String getKey() {
        if(srcTag==null || srcTag.trim().length()==0) {
            return indexName + "_" + reduceId;
        } else {
            return srcTag.trim() + "_" + indexName + "_" + reduceId;
        }
    }
}
