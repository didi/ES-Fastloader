package com.didichuxing.fastindex.common.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexOpIndexPo extends BasePo {
    private String srcTag;

    private String templateName;
    private String indexName;

    private boolean isFinish;
    private long finishTime;

    @Override
    public String getKey() {
        if(srcTag==null || srcTag.trim().length()==0) {
            return indexName;
        } else {
            return srcTag.trim() + "_" + indexName;
        }
    }
}
