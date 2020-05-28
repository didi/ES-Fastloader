package com.didichuxing.datachannel.arius.fastindex.mr;

import com.alibaba.fastjson.JSON;
import com.didichuxing.datachannel.arius.fastindex.mr.mapreduce.FastIndexMapper;
import com.didichuxing.datachannel.arius.fastindex.mr.mapreduce.FastIndexReducer;
import com.didichuxing.datachannel.arius.fastindex.mr.metrics.MetricService;
import com.didichuxing.datachannel.arius.fastindex.mr.remote.EnvEnum;
import com.didichuxing.datachannel.arius.fastindex.mr.remote.RemoteService;
import com.didichuxing.datachannel.arius.fastindex.mr.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.mr.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.HdfsUtil;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.LogUtils;
import com.didichuxing.datachannel.arius.fastindex.server.job.JobService;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;


/*
 * 入口函数，执行方式如下
 * hadoop jar  mr.jar FastIndex  ${taskConfig}
 */
public class FastIndex extends Configured implements Tool {
    private static final String MAIN_CLASS = "FastIndex";

    private TaskConfig taskConfig;
    private IndexInfo indexInfo;

    public static void main(String[] args) throws Exception {
        LogUtils.info("FastIndex jar start running");

        JobService jobService = new JobService();
        jobService.startSchedule();


        int res = ToolRunner.run(new FastIndex(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            if (args.length != 1) {
                LogUtils.info("param args > 1, lenght:" + args.length + ", args:" + StringUtils.join("#######", args));
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                sb.append(args[i]).append(" ");
            }

            // 解析taskConfig
            String taskConfigStr = sb.toString();
            LogUtils.info("taskConfig" + taskConfigStr);
            taskConfig = TaskConfig.getTaskConfig(taskConfigStr);
            taskConfig.check();
            LogUtils.info("taskConfig" + JSON.toJSONString(taskConfig));

            // 根据当前环境，配置server地址
            RemoteService.setHost(taskConfig.getSrcTag());

            // 如果任务已经结束，则清理finish标识，重新执行
            if (RemoteService.loadDataIsFinish(taskConfig.getEsTemplate(), taskConfig.getTime())) {
                LogUtils.info("remove template finish tag, template:" + taskConfig.getEsTemplate() + ", time:" + taskConfig.getTime());
                RemoteService.removeFinishTag(taskConfig.getEsTemplate(), taskConfig.getTime());
                // es需要时间刷新
                Thread.sleep(10000);
            }

            if (RemoteService.loadDataIsFinish(taskConfig.getEsTemplate(), taskConfig.getTime())) {
                throw new Exception("template finish, template:" + taskConfig.getEsTemplate() + ", time:" + taskConfig.getTime());
            }

            // 配置hadoop用户名，密码
            // FIXME 滴滴内部策略，需要修改成对应hadoop初始化逻辑
            System.setProperty("HADOOP_USER_NAME", taskConfig.getUser());
            System.setProperty("HADOOP_USER_PASSWORD", taskConfig.getPasswd());

            // 清理hdfs上的相关文件
            HdfsUtil.deleteDir(new Configuration(), taskConfig.getHdfsOutputPath());

            // 获得索引配置信息
            indexInfo = RemoteService.getIndexConfig(taskConfig.getEsTemplate(), taskConfig.getTime(), taskConfig.getHdfsSize());
            indexInfo.check(taskConfig);

            // 执行mr任务
            Configuration conf = getHdfsConfig();
            Job job = getHdfsJob(conf);
            startGetJobIdThread(job);

            LogUtils.info("################ MapReduce start, job name:" + job.getJobName());
            boolean res = job.waitForCompletion(true);
            if (res) {
                LogUtils.info("################ MapReduce success, job id:" + job.getJobID() + ", result:" + res);
                LogUtils.info("################ LoadDataToES start");

                // mr任务完成支持，Lucene文件在taskConfig.getHdfsESOutputPath()目录中，触发数据加载任务，把数据加载到ES中
                String ret = RemoteService.startLoadData(taskConfig.getEsTemplate(), taskConfig.getTime(), taskConfig.getHdfsESOutputPath(),
                        indexInfo.getExpanfactor(), taskConfig.getUser(), taskConfig.getPasswd());
                LogUtils.info("start load data, ret:" + ret);
                if (ret == null) {
                    throw new Exception("start load data error");
                }

                // 等待加载任务完成
                waitForFinish();
                LogUtils.info("################ LoadDataToES finish");

                // 清理数据，防止hdfs出现文件过多的情况
                HdfsUtil.deleteDir(new Configuration(), taskConfig.getHdfsOutputPath());

                // 提交当前reducer的metric信息
                MetricService.sendMetric(taskConfig, startTime, System.currentTimeMillis());
                LogUtils.info("################ Arius FastIndex finish");
                return 0;
            } else {

                HdfsUtil.deleteDir(new Configuration(), taskConfig.getHdfsOutputPath());

                String info = "################ MapReduce failed, job id:" + job.getJobID();
                LogUtils.error(info);
                MetricService.sendError(taskConfig, info);
                return -1;
            }
        } catch (Throwable t) {
            LogUtils.error("fast index catch exception, msg:" + t.getMessage(), t);
            MetricService.sendError(taskConfig, t);
            return -1;

        }
    }

    // 配置mr参数
    private Job getHdfsJob(Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, MAIN_CLASS);
        job.setJobName("AriusFastIndex_" + taskConfig.getEsTemplate());
        job.setJarByClass(FastIndex.class);
        job.setMapperClass(FastIndexMapper.class);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        HCatInputFormat.setInput(job, taskConfig.getHiveDB(), taskConfig.getHiveTable(), taskConfig.getFilterStr());

        job.setReducerClass(FastIndexReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(indexInfo.getReduceNum());
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(taskConfig.getHdfsMROutputPath()));

        return job;
    }

    // 配置mr参数
    private Configuration getHdfsConfig() {
        Configuration conf = getConf();
        conf.set(TaskConfig.TASKCONFIG, JSON.toJSONString(taskConfig));
        conf.set(IndexInfo.TEMPLATE_CONFIG, JSON.toJSONString(indexInfo));

        conf.setBoolean("mapreduce.map.output.compress", true); //设置map输出压缩
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);

        conf.set("mapreduce.user.classpath.first", "true");     //设置优先使用用户classpath
        conf.set("mapred.task.timeout", "8000000");             //毫秒, 默认10分钟, 加长是防止copy es数据时间过短
        conf.set("mapreduce.map.memory.mb", "6144");            // 单个map任务最多6G
        conf.set("mapreduce.reduce.memory.mb", "15360");        // 单个reducer任务最多15G

        conf.set("mapreduce.map.java.opts", "-Xmx4096m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");

        conf.set("mapreduce.map.cpu.vcores", "4");
        conf.set("mapreduce.reduce.cpu.vcores", "6");

        // 关闭MR推测执行策略
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);

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

        conf.set("yarn.app.mapreduce.am.resource.mb", "8192");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Djava.net.preferIPv4Stack=true -Xmx8192m -Xms8192m -XX:+PrintGCDetails -Xloggc:gc.log -XX:+PrintGCTimeStamps");

        conf.set("mapreduce.map.maxattempts", "4");
        conf.set("mapred.job.queue.name", taskConfig.getMrqueue());

        return conf;
    }

    /* 获得mr任务的jobId */
    private void startGetJobIdThread(Job job) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    if (job.getJobID() != null) {
                        LogUtils.info("map reducer jobId:" + job.getJobID());
                        break;
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }


            }
        });

        thread.setDaemon(true);
        thread.setName("getJobId");
        thread.start();
    }

    /* 等待数据加载任务结束 */
    private void waitForFinish() throws Exception {
        int i = 3 * 60;
        while (i > 0) {
            i--;

            if (RemoteService.loadDataIsFinish(taskConfig.getEsTemplate(), taskConfig.getTime())) {
                return;
            }

            try {
                Thread.sleep(60 * 1000);
            } catch (Throwable t) {
                LogUtils.error(t.getMessage(), t);
            }
        }

        throw new Exception("wait for load data finish time out");

    }
}
