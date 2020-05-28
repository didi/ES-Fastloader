package com.didichuxing.fastindex.common.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/* 对应一个索引的数据加载任务 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexOpIndexPo extends BasePo {
    private String templateName;    // 索引名
    private String indexName;       // 模板名

    private boolean isFinish;       // 是否已经完成
    private long finishTime;        // 完成时间

    @Override
    public String getKey() {
        return indexName;
    }
}
