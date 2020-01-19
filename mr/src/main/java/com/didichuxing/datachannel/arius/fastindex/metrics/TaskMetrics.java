package com.didichuxing.datachannel.arius.fastindex.metrics;


import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class TaskMetrics {
    private long readBytes   =0;     // 任务采集的总的数据量大小
    private long readRecords =0;     // 读取条数
    private long errorRecords=0;     // 失败条数
    private long writeRecords=0;     // 写入条数

    @JSONField(serialize = false)
    public void addReadBytes(long rb) {
        synchronized (this) {
            this.readBytes += rb;
        }
    }

    @JSONField(serialize = false)
    public void addReadRecords(long count) {
        synchronized (this) {
            this.readRecords += count;
        }
    }

    @JSONField(serialize = false)
    public void addErrorRecords(long count) {
        synchronized (this) {
            this.errorRecords += count;
        }
    }

    @JSONField(serialize = false)
    public void addWriteRecords(long count) {
        synchronized (this) {
            this.writeRecords += count;
        }
    }
}
