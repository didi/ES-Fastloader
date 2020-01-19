package com.didichuxing.datachannel.arius.fastindex.metrics;

import com.alibaba.fastjson.JSON;
import com.didichuxing.datachannel.arius.fastindex.remote.RemoteService;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;

import java.text.SimpleDateFormat;
import java.util.Map;

public class MetricService {

    public static void sendError(TaskConfig taskConfig, Throwable t) {
        sendError(taskConfig, t.getMessage());
    }

    public static void sendError(TaskConfig taskConfig, String msg) {
        LogUtils.error("MetricService sendError, taskConfig" + JSON.toJSONString(taskConfig) + ", msg:" + msg);
        FinishMetrics finishMetrics = new FinishMetrics(taskConfig);
        finishMetrics.setIsSuccess(false);
        finishMetrics.setErrorMessage(msg);

        // TODO 替换成结果信息发送逻辑
        // KafkaClient.send(JSON.toJSONString(finishMetrics));
    }

    private static final SimpleDateFormat dateFormat         = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static void sendMetric(TaskConfig taskConfig, long start, long end) {
        long cost = (end-start)/1000;
        if(cost<=0) {
            cost=1;
        }

        FinishMetrics finishMetrics = new FinishMetrics(taskConfig);
        finishMetrics.setIsSuccess(true);
        finishMetrics.setStartTime(dateFormat.format(start));
        finishMetrics.setEndTime(dateFormat.format(end));
        finishMetrics.setTotalCosts(cost);

        long writeRecord = 0;
        long readRecord = 0;
        long errorRecord = 0;
        long readBytes = 0;

        Map<String, TaskMetrics> taskMetricsMap = RemoteService.getMetric(taskConfig.getEsTemplate(), taskConfig.getTime());
        for (String shard : taskMetricsMap.keySet()) {
            TaskMetrics taskMetrics = taskMetricsMap.get(shard);
            writeRecord += taskMetrics.getWriteRecords();
            readRecord += taskMetrics.getReadRecords();
            errorRecord += taskMetrics.getErrorRecords();
            readBytes += taskMetrics.getReadBytes();
        }

        finishMetrics.setByteSpeedPerSecond(readBytes / cost);
        finishMetrics.setRecordSpeedPerSecond(readRecord / cost);
        finishMetrics.setTotalReadBytes(readBytes);

        finishMetrics.setTotalReadRecords(readRecord);      // 读出总条数
        finishMetrics.setTotalWriteRecords(writeRecord);     // 写入总条数
        finishMetrics.setTotalErrorRecords(errorRecord);     // 读写失败总数

        // TODO 替换成结果信息发送逻辑
        // KafkaClient.send(JSON.toJSONString(finishMetrics));
    }
}
