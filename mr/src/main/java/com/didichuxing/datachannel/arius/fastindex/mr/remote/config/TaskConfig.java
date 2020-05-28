package com.didichuxing.datachannel.arius.fastindex.mr.remote.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.HdfsUtil;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.LogUtils;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.conf.Configuration;

import java.text.SimpleDateFormat;
import java.util.*;

/*
 * fastIndex任务的配置信息
 * 通过hadoop jar mr.jar  FastIndex $taskConfig 传入
 */
@Data
public class TaskConfig {
    public static final String TASKCONFIG = "taskConfig";

    private long taskId;                // 任务id

    private String hdfsInputPath;       // hive表对应的hdfs路径, 用于获得Hive数据大小
    private String hiveDB;              // hive库名
    private String hiveTable;           // hive表名
    private String hiveColumns;         // hive表结构
    private Set<String>  intColumns = new HashSet<>();  // hive表中，类型为int的字段名集合

    private String esTemplate;          // es模板名

    private String key;                 // es索引主键名，多个字段名以逗号分隔

    private Map<String, String> filter; // hive的过滤字段以及对应的value
    private long time;                  // es索引分区时间

    private String hdfsOutputPath;      // reducer产生的lucene文件在hdfs上的存储路径

    private String user;                // hive用户名
    private String passwd;              // hive密码

    private String mrqueue;             // hive计算队列

    private String env = "online";      // 运行环境
    private Integer batchSize = 500;    // reducer任务中单次写入es的数据个数
    private Integer threadPoolSize = 4; // reducer任务中写入线程个数

    @JSONField(serialize = false)
    public void check() throws Exception {
        if(StringUtils.isBlank(hiveDB) ||
                StringUtils.isBlank(hiveTable) ||
                StringUtils.isBlank(esTemplate) ||
                StringUtils.isBlank(hdfsOutputPath) ||
                StringUtils.isBlank(user) ||
                StringUtils.isBlank(passwd) ||
                StringUtils.isBlank(mrqueue) ||
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
