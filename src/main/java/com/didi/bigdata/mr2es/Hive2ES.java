package com.didi.bigdata.mr2es;


import com.alibaba.fastjson.JSONObject;
import com.didi.bigdata.mr2es.alarm.AlarmUtil;
import com.didi.bigdata.mr2es.es.ESSearch;
import com.didi.bigdata.mr2es.mr.Hive2ESMapper;
import com.didi.bigdata.mr2es.mr.Hive2ESReducer;
import com.didi.bigdata.mr2es.service.FastIndexService;
import com.didi.bigdata.mr2es.utils.Cmd;
import com.didi.bigdata.mr2es.utils.Constants;
import com.didi.bigdata.mr2es.utils.DateUtils;
import com.didi.bigdata.mr2es.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.util.Map;


/**
 * Created by WangZhuang on 2019/1/21
 */
@Slf4j
public class Hive2ES extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Hive2ES(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Cmd cmd = new Cmd(Constants.MAIN_CLASS);
        checkParams(cmd, args);
        Configuration conf = getConf();
        setConfParams(cmd, conf);

        Job job = Job.getInstance(conf, Constants.MAIN_CLASS);
        job.setJarByClass(Hive2ES.class);
        job.setMapperClass(Hive2ESMapper.class);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        HCatInputFormat.setInput(job, cmd.getArgValue(Constants.KEY_DB),
                cmd.getArgValue(Constants.KEY_TABLE), filter(cmd));
        job.setReducerClass(Hive2ESReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(Integer.parseInt(conf
                .get(Constants.KEY_REDUCE_NUM)));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,
                new Path(cmd.getArgValue(Constants.KEY_MR_OUTPUT_PATH)));
        try {
            boolean res = job.waitForCompletion(true);
            if (res) {
                log.info("mr2es process success!");
                noticeEs(conf);
                return 0;
            } else {
                log.info("mr2es process error! please check!");
                AlarmUtil.getAlarmUtil().exec(Constants.ALARM_ID
                                + conf.get(Constants.KEY_WORKFLOW_NAME),
                        "mr2es process error! please check!");
                return -1;
            }
        } catch (Exception e) {
            log.error("mr2es catch exception", e);
            AlarmUtil.getAlarmUtil().exec(Constants.ALARM_ID
                            + conf.get(Constants.KEY_WORKFLOW_NAME),
                    "mr2es process exception!");
            return -1;
        }
    }

    /**
     * 通知es方开始load数据
     *
     * @param configuration
     */
    private void noticeEs(Configuration configuration) {
        String template = configuration.get(Constants.KEY_WORKFLOW_NAME);
        long time = DateUtils.getTimestamp(configuration.get(Constants.KEY_DT));
        String esDir = configuration.get(Constants.KEY_ES_OUTPUT_PATH);
        boolean res = FastIndexService.getInstance()
                .startLoadData(template, time, esDir);
        if (res) {
            checkLoadIsFinish(configuration, template, time);
        } else {
            AlarmUtil.getAlarmUtil().exec(Constants.ALARM_ID + template,
                    "notice es start load data error,please check!");
        }
    }

    /**
     * 检查es数据是否生成成功, 30s检查一次直到成功
     */
    private void checkLoadIsFinish(Configuration configuration,
                                   String template, long time) {
        log.info("start check load is finished...");
        String indexName = template + configuration.get(Constants.KEY_DT)
                .substring(4);
        while (true) {
            boolean res = FastIndexService.getInstance()
                    .isFinishes(template, time);
            if (res) {
                boolean search = search(configuration);
                if (search) {
                    AlarmUtil.getAlarmUtil().exec("es fastload 成功: "
                                    + indexName,"乘客宽表ES数据构建成功,请知晓!");
                    updateIndexName(configuration);
                } else {
                    AlarmUtil.getAlarmUtil().exec("es fastload 失败: "
                                    + indexName,"乘客宽表ES数据构建成功,但预查询失败,请立刻检查!");
                }
                break;
            }
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("check load is finished!");
    }

    /**
     * 预search
     * @param configuration
     * @return
     */
    private boolean search(Configuration configuration) {
        int tryCount = 3;
        ESSearch esSearch = new ESSearch();
        String indexName = "bigpassenger"
                + configuration.get(Constants.KEY_DT).substring(4);
        for (int i = 0; i < tryCount; i++) {
            int count;
            try {
                count = esSearch.getCount(indexName);
                log.info("pre search result is:{}", count);
                if (count > 10000) {
                    return true;
                }
            } catch (Exception e) {
                log.info("pre search error:{}", e);
            }
        }
        return false;
    }

    /**
     * 切换标签系统索引名
     */
    private void updateIndexName(Configuration configuration) {
        //todo:切换标签系统索引名
    }


    /**
     * hive过滤条件
     *
     * @param cmd
     * @return
     */
    private String filter(Cmd cmd) {
        String dt = cmd.getArgValue(Constants.KEY_DT);
        String filterStr = "year='" + dt.substring(0, 4) + "' and month='"
                + dt.substring(4, 6) + "' and day='" + dt.substring(6, 8) + "'";
        return filterStr;
    }

    /**
     * 设置环境参数
     *
     * @param cmd
     * @param conf
     */
    private void setConfParams(Cmd cmd, Configuration conf) {
        conf.set(Constants.KEY_REDUCE_NUM,
                cmd.getArgValue(Constants.KEY_REDUCE_NUM));
        conf.set(Constants.KEY_DB, cmd.getArgValue(Constants.KEY_DB));
        conf.set(Constants.KEY_TABLE, cmd.getArgValue(Constants.KEY_TABLE));
        conf.set(Constants.KEY_INDEX, cmd.getArgValue(Constants.KEY_INDEX));
        conf.set(Constants.KEY_TYPE, cmd.getArgValue(Constants.KEY_TYPE));
        conf.set(Constants.KEY_ES_OUTPUT_PATH,
                cmd.getArgValue(Constants.KEY_ES_OUTPUT_PATH));
        conf.set(Constants.KEY_ID, cmd.getArgValue(Constants.KEY_ID));
        conf.set(Constants.KEY_ES_WORK_DIR,
                cmd.getArgValue(Constants.KEY_ES_WORK_DIR));
        conf.set(Constants.KEY_ES_NODE_NAME,
                cmd.getArgValue(Constants.KEY_ES_NODE_NAME));
        conf.set(Constants.KEY_REPLICAS_SHARDS_NUMBER,
                cmd.getArgValue(Constants.KEY_REPLICAS_SHARDS_NUMBER));
        conf.set(Constants.KEY_DT, cmd.getArgValue(Constants.KEY_DT));
        conf.set(Constants.KEY_USER_TYPE,
                cmd.getArgValue(Constants.KEY_USER_TYPE));
        conf.set(Constants.KEY_WORKFLOW_NAME,
                cmd.getArgValue(Constants.KEY_WORKFLOW_NAME));
        setIndexJsonShards(cmd, conf);

        conf.setBoolean("mapreduce.map.output.compress", true); //设置map输出压缩
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC,
                GzipCodec.class, CompressionCodec.class);

        conf.set("mapreduce.user.classpath.first", "true"); //设置优先使用用户classpath
        conf.set("mapred.task.timeout", "8000000"); //毫秒, 默认10分钟, 加长是防止copy es数据时间过短
        conf.set("mapreduce.map.memory.mb", "6144");
        conf.set("mapreduce.reduce.memory.mb", "15360");
        conf.set("mapreduce.map.java.opts", "-Xmx4096m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx14336m");

        conf.set("mapreduce.job.running.map.limit", "1000");
        conf.set("mapreduce.job.running.reduce.limit", "2500"); //调大reduce同时执行个数
        conf.set("mapreduce.task.io.sort.mb", "1024");
        conf.set("dfs.datanode.max.transfer.threads", "8192");
        conf.set("mapreduce.tasktracker.http.threads", "1000");
        conf.set("mapreduce.shuffle.max.threads", "50");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "500");
        conf.set("mapreduce.task.io.sort.factor", "500");
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.01");

        conf.set("dfs.datanode.handler.count", "20");

        conf.set("mapreduce.map.cpu.vcores", "4");
        conf.set("mapreduce.reduce.cpu.vcores", "6");

        conf.set("yarn.app.mapreduce.am.resource.mb", "8192");
        conf.set("yarn.app.mapreduce.am.command-opts",
                "-Djava.net.preferIPv4Stack=true -Xmx8192m -Xms8192m " +
                        "-XX:+PrintGCDetails -Xloggc:gc.log " +
                        "-XX:+PrintGCTimeStamps");

    }


    /**
     * 设置index json 和 shards数
     *
     * @param cmd
     * @param conf
     */
    private void setIndexJsonShards(Cmd cmd, Configuration conf) {
        long time = DateUtils.getTimestamp(cmd.getArgValue(Constants.KEY_DT));
        Map<String, String> map = FastIndexService.getInstance()
                .getIndex(cmd.getArgValue(Constants.KEY_WORKFLOW_NAME), time);
        String shardsNum = map.get(Constants.KEY_REDUCE_NUM);
        String indexConfig = map.get(Constants.KEY_INDEX_CONFIG);
        if (StringUtils.isBlank(shardsNum) ||
                StringUtils.isBlank(indexConfig)) {
            log.error("index config or shards is invalid, job exit!");
            System.exit(-1);
        }
        conf.set(Constants.KEY_REDUCE_NUM, shardsNum);
        conf.set(Constants.KEY_INDEX_CONFIG, indexConfig);
    }

    /**
     * 检查输入参数
     *
     * @param cmd
     * @param args
     */
    private void checkParams(Cmd cmd, String[] args) {
        cmd.addParam(Constants.KEY_DB, "hive库名");
        cmd.addParam(Constants.KEY_TABLE, "hive表名");
        cmd.addParam(Constants.KEY_INDEX, "索引index");
        cmd.addParam(Constants.KEY_TYPE, "索引type");
        cmd.addParam(Constants.KEY_DT, "yyyymmdd格式日期");
        cmd.addParam(Constants.KEY_REDUCE_NUM, "reduce数量");
        cmd.addParam(Constants.KEY_MR_OUTPUT_PATH, "mr输出路径");
        cmd.addParam(Constants.KEY_ES_OUTPUT_PATH, "es输出路径");
        cmd.addParam(Constants.KEY_ID, "主键id");
        cmd.addParam(Constants.KEY_ES_WORK_DIR, "es节点数据的临时相对路径");
        cmd.addParam(Constants.KEY_ES_NODE_NAME, "es节点名称");
        cmd.addParam(Constants.KEY_REPLICAS_SHARDS_NUMBER, "es备份分片数");
        cmd.addParam(Constants.KEY_WORKFLOW_NAME, "工作流");
        cmd.addParam(Constants.KEY_USER_TYPE, "用户类型");
        if (args.length == 0) {
            cmd.printHelp();
            System.exit(-1);
        }
        cmd.parse(args);
    }
}
