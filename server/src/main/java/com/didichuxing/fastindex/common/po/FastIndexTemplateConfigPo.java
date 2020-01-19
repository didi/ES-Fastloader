package com.didichuxing.fastindex.common.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexTemplateConfigPo extends BasePo {
    private String name;

    private int expanfactor = -1;

    private String transformType = "normal";

    @Override
    public String getKey() {
        return name;
    }
}
