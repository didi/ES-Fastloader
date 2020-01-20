package com.didichuxing.datachannel.arius.fastindex.remote.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.didichuxing.datachannel.arius.fastindex.utils.HdfsUtil;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.conf.Configuration;

import java.text.SimpleDateFormat;
import java.util.*;

@Data
public class TaskConfig {
    public static final String TASKCONFIG = "taskConfig";

    private String jobId;
    private long taskId;

    private String hdfsInputPath;       // hive对应hdfs路径
    private String hiveDB;              // hive库名
    private String hiveTable;           // hive表名
    private String hiveColumns;         // hive表结构
    private Set<String>  intColumns = new HashSet<>();

    private String esTemplate;          // 索引index

    private String key;                 // 主键

    private Map<String, String> filter;
    private long time;                  // 数据对应的时间

    private String hdfsOutputPath;      // mr输出路径

    private String user;                // hive用户名
    private String passwd;              // hive密码

    private String mrqueue;             // hive计算队列
    private String metricTopic;         // 统计信息topic

    /* kafka相关 */
    private String kafkaServer;         // kafka server地址
    private String kafkaClusterId;
    private String kafkaAppId;
    private String kafkaPassword;

    private String env = "online";
    private Integer batchSize = 500;
    private Integer threadPoolSize = 4;

    @JSONField(serialize = false)
    public void check() throws Exception {
        if(StringUtils.isBlank(hiveDB) ||
                StringUtils.isBlank(hiveTable) ||
                StringUtils.isBlank(esTemplate) ||
                StringUtils.isBlank(hdfsOutputPath) ||
                StringUtils.isBlank(user) ||
                StringUtils.isBlank(passwd) ||
                StringUtils.isBlank(mrqueue) ||
                StringUtils.isBlank(metricTopic) ||
                StringUtils.isBlank(kafkaServer) ||
                StringUtils.isBlank(kafkaClusterId) ||
                StringUtils.isBlank(kafkaAppId) ||
                StringUtils.isBlank(kafkaPassword) ||
                time<=0) {
            throw new Exception("param is wrong");
        }

        if(!StringUtils.isBlank(key)) {
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

        if(hdfsOutputPath.endsWith("/")) {
            hdfsOutputPath = hdfsOutputPath.substring(0, hdfsOutputPath.length()-1);
        }

        // 拼接实际使用的路径
        String srcTag = getSrcTag();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String pathtag =srcTag.trim() +"_"+ esTemplate.trim() +"_"+ dateFormat.format(time);
        if(pathtag.length()>100) {
            pathtag = pathtag.substring(0, 100);
        }

        hdfsOutputPath = hdfsOutputPath + "/" + pathtag;


        // 解析表结构，提取数据类型的字段
        try {
            JSONArray jsonArray = JSONArray.parseArray(hiveColumns);
            for (Object obj : jsonArray) {
                JSONObject columnObj = (JSONObject) obj;
                String name = columnObj.getString("name");
                String type = columnObj.getString("type");

                if("int".equals(type) || "bigint".equals(type)) {
                    intColumns.add(name);
                }
            }

            LogUtils.info("intColumns:" + intColumns);
        } catch (Throwable t) {
            LogUtils.error("解析hive column报错, hiveColums:" + hiveColumns, t);
        }
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
                    if (intColumns.contains(key)) {
                        str = str + key + "=" + v;
                    } else {
                        str = str + key + "='" + v  + "'";
                    }

                    str = str + " or ";
                }
                str = str.substring(0, str.length()-4);

                str = str + " )";
                sb.append(str);

            } else {
                if (intColumns.contains(key)) {
                    sb.append(key).append("=").append(filter.get(key));

                } else {
                    sb.append(key).append("=").append("'").append(filter.get(key)).append("'");
                }
            }

            sb.append(" and ");
        }

        String ret = sb.substring(0, sb.length()-5);
        LogUtils.info("filter :" + ret);
        return ret;
    }

    @JSONField(serialize = false)
    public String getSrcTag() throws Exception {
        String filterStr = null;
        if (filter != null && filter.size() > 0) {
            List<String> filterList = new ArrayList<>();
            for (String key : filter.keySet()) {
                filterList.add(filter.get(key).replace(",", "_"));
            }

            Collections.sort(filterList);
            filterStr = String.join("_", filterList);
        }

        String ret = hiveDB + "-" + hiveTable;
        if (filterStr != null) {
            ret = ret + "-" + filterStr;
        }

        if(ret.length()>100) {
            ret = ret.substring(0, 100);
        }

        return ret;
    }

    @JSONField(serialize = false)
    public long getHdfsSize() {
        if(hdfsInputPath==null || hdfsInputPath.trim().length()==0) {
            return -1;
        }

        return HdfsUtil.getSize(new Configuration(), hdfsInputPath);
    }

    public static TaskConfig getTaskConfig(JobContext context) {
        String str = context.getConfiguration().get(TaskConfig.TASKCONFIG);
        TaskConfig taskConfig = JSON.parseObject(str, TaskConfig.class);
        if(taskConfig!=null) {
            LogUtils.addIgnoreStr(taskConfig.kafkaPassword);
            LogUtils.addIgnoreStr(taskConfig.passwd);
        }

        return taskConfig;
    }

    public static TaskConfig getTaskConfig(String str) {
        TaskConfig taskConfig = JSON.parseObject(str, TaskConfig.class);
        if (taskConfig != null) {
            LogUtils.addIgnoreStr(taskConfig.kafkaPassword);
            LogUtils.addIgnoreStr(taskConfig.passwd);
        }

        return taskConfig;
    }
}
