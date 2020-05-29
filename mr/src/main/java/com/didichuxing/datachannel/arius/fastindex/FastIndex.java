package com.didichuxing.datachannel.arius.fastindex;

import com.didichuxing.datachannel.arius.fastindex.remote.RemoteService;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.utils.HdfsUtil;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
 * 入口函数，执行方式如下
 * hadoop jar mr.jar com.didichuxing.datachannel.arius.fastindex.FastIndex  ${taskConfig}
 */
public class FastIndex extends Configured implements Tool {

    private TaskConfig taskConfig;
    private IndexInfo indexInfo;

    public static void main(String[] args) throws Exception {
        LogUtils.info("FastIndex jar start running");
        int res = ToolRunner.run(new FastIndex(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) {
        try {
            taskConfig = getTaskConfig(args);

            // 根据当前环境，配置server地址
            RemoteService.setHost(taskConfig.getServer());

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
            indexInfo = RemoteService.getIndexConfig(taskConfig.getEsTemplate(), taskConfig.getTime());
            indexInfo.check(taskConfig);

            // 执行mr任务
            Configuration conf = getConf();
            HdfsUtil.setHdfsConfig(conf, taskConfig, indexInfo);
            Job job = HdfsUtil.getHdfsJob(conf, taskConfig, indexInfo);
            startGetJobIdThread(job);

            LogUtils.info("################ MapReduce start, job name:" + job.getJobName());
            boolean res = job.waitForCompletion(true);
            if (res) {
                LogUtils.info("################ MapReduce success AND start LoadData to es node");

                // mr任务完成支持，Lucene文件在taskConfig.getHdfsESOutputPath()目录中，触发数据加载任务，把数据加载到ES中
                String ret = RemoteService.startLoadData(
                        taskConfig.getEsTemplate(),
                        taskConfig.getTime(),
                        taskConfig.getHdfsESOutputPath(),
                        indexInfo.getReducerNum(),
                        taskConfig.getUser(),
                        taskConfig.getPasswd(),
                        taskConfig.getEsWorkDir()
                );
                if (ret == null) {
                    throw new Exception("start load data error");
                }

                LogUtils.info("start load data, ret:" + ret);

                // 等待加载任务完成
                waitForFinish();

                // 清理数据，防止hdfs出现文件过多的情况
                HdfsUtil.deleteDir(new Configuration(), taskConfig.getHdfsOutputPath());
                LogUtils.info("################ Arius FastIndex finish");
                return 0;
            } else {
                HdfsUtil.deleteDir(new Configuration(), taskConfig.getHdfsOutputPath());
                String info = "################ MapReduce failed, job id:" + job.getJobID();
                LogUtils.error(info);
                return -1;
            }
        } catch (Throwable t) {
            LogUtils.error("fast index catch exception, msg:" + t.getMessage(), t);
            return -1;

        }
    }

    private TaskConfig getTaskConfig(String[] args) throws Exception {
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
        TaskConfig taskConfig = TaskConfig.getTaskConfig(taskConfigStr);
        taskConfig.check();
        return taskConfig;
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
