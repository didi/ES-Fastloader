package com.didichuxing.datachannel.arius.fastindex.server.common.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/* 针对模板的特殊配置 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexTemplateConfigPo extends BasePo {
    private String name;                        // 模板名
    private int expanfactor = -1;               // redcuer任务数目=shardNum*expanfactor
    private String transformType = "normal";    // 数据转化逻辑

    @Override
    public String getKey() {
        return name;
    }
}
