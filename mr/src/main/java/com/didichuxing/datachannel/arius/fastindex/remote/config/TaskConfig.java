package com.didichuxing.datachannel.arius.fastindex.remote.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.JobContext;

import java.text.SimpleDateFormat;
import java.util.*;

/*
 * fastIndex任务的配置信息
 * 通过hadoop jar mr.jar  com.didichuxing.datachannel.arius.fastindex.FastIndex $taskConfig 传入
 */
@Data
public class TaskConfig {
    public static final String TASKCONFIG = "taskConfig";

    private String esTemplate;          // es模板名
    private long time;                  // es索引分区时间

    private String hiveDB;              // hive库名
    private String hiveTable;           // hive表名
    private String key;                 // es索引主键名，多个字段名以逗号分隔
    private Map<String, String> filter; // hive的过滤字段以及对应的value
    private String mrqueue;             // hive计算队列
    private String hdfsOutputPath;      // reducer产生的lucene文件在hdfs上的存储路径
    private String user;                // hive用户名
    private String passwd;              // hive密码
    private String esWorkDir;           // es dataNode上工作目录

    private String server;                // 服务端地址
    private Integer batchSize = 500;    // reducer任务中单次写入es的数据个数
    private Integer threadPoolSize = 4; // reducer任务中写入线程个数

    @JSONField(serialize = false)
    public void check() throws Exception {
        if (StringUtils.isBlank(esTemplate) ||
                StringUtils.isBlank(hiveDB) ||
                StringUtils.isBlank(hiveTable) ||
                StringUtils.isBlank(key) ||
                StringUtils.isBlank(mrqueue) ||
                StringUtils.isBlank(hdfsOutputPath) ||
                StringUtils.isBlank(user) ||
                StringUtils.isBlank(passwd) ||
                StringUtils.isBlank(esWorkDir) ||
                StringUtils.isBlank(server) ||
                time <= 0) {
            throw new Exception("param is wrong");
        }

        if (!StringUtils.isBlank(key)) {
            List<String> keyList = getKeyList();
            if (keyList == null || keyList.size() == 0) {
                throw new Exception("key is blank, key:" + key);
            }

            for (String k : keyList) {
                if (StringUtils.isBlank(k)) {
                    throw new Exception("key is blank, key:" + key);
                }
            }
        }

        if (hdfsOutputPath.endsWith("/")) {
            hdfsOutputPath = hdfsOutputPath.substring(0, hdfsOutputPath.length() - 1);
        }

        // 拼接实际使用的路径
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String pathtag = esTemplate.trim() + "_" + dateFormat.format(time);
        if (pathtag.length() > 100) {
            pathtag = pathtag.substring(0, 100);
        }

        hdfsOutputPath = hdfsOutputPath + "/" + pathtag;
    }

    @JSONField(serialize = false)
    public String getHdfsMROutputPath() {
        return hdfsOutputPath + "/mr";
    }

    @JSONField(serialize = false)
    public String getHdfsESOutputPath() {
        return hdfsOutputPath + "/es";
    }

    @JSONField(serialize = false)
    public List<String> getKeyList() {
        List<String> ret = new ArrayList<>();
        if(StringUtils.isBlank(key)) {
            return ret;
        }

        for(String k : key.split(",")) {
            ret.add(k);
        }

        return ret;
    }

    @JSONField(serialize = false)
    public String getFilterStr() throws Exception {
        if(filter==null || filter.size()==0) {
            throw new Exception("filter is blank");
        }

        StringBuilder sb = new StringBuilder();
        for(String key : filter.keySet()) {
            String value = filter.get(key);
            if(value.contains(",")) {
                // 处理OR的情况
                String[] values = value.split(",");

                String str = "( ";
                for(String v : values) {
                    str = str + key + "='" + v  + "'";
                    str = str + " or ";
                }
                str = str.substring(0, str.length()-4);
                str = str + " )";
                sb.append(str);

            } else {
                sb.append(key).append("=").append("'").append(filter.get(key)).append("'");
            }

            sb.append(" and ");
        }

        String ret = sb.substring(0, sb.length()-5);
        LogUtils.info("filter :" + ret);
        return ret;
    }

    public static TaskConfig getTaskConfig(JobContext context) {
        String str = context.getConfiguration().get(TaskConfig.TASKCONFIG);
        TaskConfig taskConfig = JSON.parseObject(str, TaskConfig.class);
        if(taskConfig!=null) {
            LogUtils.addIgnoreStr(taskConfig.passwd);
        }

        return taskConfig;
    }

    public static TaskConfig getTaskConfig(String str) {
        TaskConfig taskConfig = JSON.parseObject(str, TaskConfig.class);
        if (taskConfig != null) {
            LogUtils.addIgnoreStr(taskConfig.passwd);
        }

        return taskConfig;
    }
}
