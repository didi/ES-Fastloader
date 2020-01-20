package com.didichuxing.fastindex.common.po;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/* 表示单个shard的数据加载任务 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexLoadDataPo extends BasePo {
    private String templateName;        // 模板名
    private String indexName;           // 索引名
    private String indexUUID;           // 索引UUID
    private long shardNum;              // 对应的shard编号
    private String hostName;            // shard属于的主机名
    private int port;                   // es http端口号

    private String srcTag;              // srcTag，处理hive的不同分区同时导入同一个索引的情况
    private String redcueIds;           // 对应的reducer编号，会有多个
    private String hdfsSrcDir;          // hdfs上存放lucene文件的路径
    private String hdfsUser;            // hdfs用户名
    private String hdfsPassword;        // hdfs密码

    private String esDstDir;            // es的工作目录

    private long zeusTaskId;            // zeus任务id，用于获得任务执行情况
    private boolean isStart;            // zeus是否已经开始执行
    private long startTime;             // zeus任务开始执行时间
    private boolean isFinish;           // zeus是否已经执行成功
    private long finishTime;            // zeus执行成功的时间
    private long runCount=0;            // zeus重试的次数

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
