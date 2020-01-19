package com.didichuxing.fastindex.common.po;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexLoadDataPo extends BasePo {
    private String templateName;
    private String indexName;
    private String indexUUID;
    private long shardNum;
    private String hostName;
    private int port;

    private String srcTag;
    private String hdfsShards;
    private String hdfsSrcDir;
    private String hdfsUser;
    private String hdfsPassword;

    private String esDstDir;

    private long zeusTaskId;
    private boolean isStart;
    private long startTime;


    private boolean isFinish;
    private long finishTime;

    // 重试次数
    private long runCount=0;


    @JSONField(serialize = false)
    public String getOpKey() {
        if (srcTag == null || srcTag.trim().length() == 0) {
            return indexName;
        } else {
            return srcTag.trim() + "_" + indexName;
        }
    }

    @Override
    public String getKey() {
        if(srcTag==null || srcTag.trim().length()==0) {
            return indexName + "_" + shardNum;
        } else {
            return srcTag.trim() + "_" + indexName + "_" + shardNum;
        }
    }
}
