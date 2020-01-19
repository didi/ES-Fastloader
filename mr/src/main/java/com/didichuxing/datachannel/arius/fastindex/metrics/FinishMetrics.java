package com.didichuxing.datachannel.arius.fastindex.metrics;


import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.utils.HostUtils;
import lombok.Data;

@Data
public class FinishMetrics {
    private String jobId;
    private long taskId;

    private String startTime;           // 任务启动时刻
    private String endTime;             // 任务结束时刻
    private long totalCosts;            // 任务总计耗时

    private long byteSpeedPerSecond;    // 任务平均流量
    private long recordSpeedPerSecond;  // 记录平均流量
    private long totalReadBytes;        // 任务采集的总的数据量大小

    private long totalReadRecords;      // 读出总条数
    private long totalWriteRecords;     // 写入总条数
    private long totalErrorRecords;     // 读写失败总数


    private String readerPluginName = "AriusFastIndexReader";
    private String writerPluginName = "AriusFastIndexWriter";

    private Boolean isSuccess = true;   // 任务运行结果

    private String errorMessage;        // 错误的原因描述

    private String hostname;            // 执行的主机


    public FinishMetrics() {}

    public FinishMetrics(TaskConfig taskConfig) {
        this.jobId = taskConfig.getJobId();
        this.taskId = taskConfig.getTaskId();
        this.hostname = HostUtils.HOSTNAME;
    }
}
