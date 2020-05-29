package com.didichuxing.datachannel.arius.fastindex.utils;

import com.alibaba.fastjson.JSON;
import com.didichuxing.datachannel.arius.fastindex.FastIndex;
import com.didichuxing.datachannel.arius.fastindex.mapreduce.FastIndexMapper;
import com.didichuxing.datachannel.arius.fastindex.mapreduce.FastIndexReducer;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.*;

/**
 * Created by WangZhuang on 2019/2/26
 */
@Slf4j
public class HdfsUtil {


    // 配置mr参数
    private static final String MAIN_CLASS = "FastIndex";
    public static Job getHdfsJob(Configuration conf, TaskConfig taskConfig, IndexInfo indexInfo) throws Exception {
        Job job = Job.getInstance(conf, MAIN_CLASS);
        job.setJobName("DidiFastIndex_" + taskConfig.getEsTemplate());
        job.setJarByClass(FastIndex.class);
        job.setMapperClass(FastIndexMapper.class);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        HCatInputFormat.setInput(job, taskConfig.getHiveDB(), taskConfig.getHiveTable(), taskConfig.getFilterStr());

        job.setReducerClass(FastIndexReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(indexInfo.getReducerNum());
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(taskConfig.getHdfsMROutputPath()));

        return job;
    }

    // 配置mr参数
    public static Configuration setHdfsConfig(Configuration conf, TaskConfig taskConfig, IndexInfo indexInfo) {
        conf.set(TaskConfig.TASKCONFIG, JSON.toJSONString(taskConfig));
        conf.set(IndexInfo.INDEX_CONFIG, JSON.toJSONString(indexInfo));

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


    /**
     * 删除hdfs文件夹
     *
     * @param conf
     * @param dir
     */
    public static boolean deleteDir(Configuration conf, String dir) {
        LogUtils.info("delete dir:" + dir);
        FileSystem fs = null;
        boolean res = false;
        try {
            fs = FileSystem.newInstance(conf);
            res = fs.delete(new Path(dir), true);
        } catch (Exception e) {
            log.error("dir delete error!dir:{}", dir);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * 创建hdfs目录
     *
     * @param dir
     * @throws IOException
     */
    public static boolean mkdir(String dir, Configuration conf) {
        FileSystem fs = null;
        boolean res = false;
        try {
            Path srcPath = new Path(dir);
            fs = srcPath.getFileSystem(conf);
            res = fs.mkdirs(srcPath);
        } catch (Exception e) {
            log.error("mkdir error!dir:{}", dir);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * 拷贝本地"文件夹"到hdfs上
     *
     * @param src
     * @param dst
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean copyDirectory(String src, String dst, Configuration conf) throws Exception {
        Path path = new Path(dst);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        File file = new File(src);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                String fname = f.getName();
                if (dst.endsWith("/")) {
                    copyDirectory(f.getPath(), dst + fname + "/", conf);
                } else {
                    copyDirectory(f.getPath(), dst + "/" + fname + "/", conf);
                }
            } else {
                uploadFile(f.getPath(), dst, conf);
            }
        }
        return true;
    }

    /**
     * 拷贝本地"文件"到hdfs上
     *
     * @param localSrc
     * @param dst
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean uploadFile(String localSrc, String dst, Configuration conf) throws Exception {
        try {
            File file = new File(localSrc);
            dst = dst + "/" + file.getName();
            Path path = new Path(dst);
            FileSystem fs = path.getFileSystem(conf);
            fs.exists(path);
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            OutputStream out = fs.create(new Path(dst));
            IOUtils.copyBytes(in, out, 8092, true);
            in.close();
        } catch (Exception e) {
            LogUtils.error("copy file error!local:" + localSrc, e);
            throw e;
        }
        return true;
    }

    /*
     * 获得hdfs路径下总磁盘大小
     */
    public static long getSize(Configuration conf, String dir) {
        if(dir==null || dir.trim().length()==0) {
            return -1;
        }

        if(dir.contains("user")) {
            // 去除user前面的路径
            int index = dir.indexOf("user");
            dir = "/" + dir.substring(index);
        }

        FileSystem fs = null;
        try {
            fs = FileSystem.newInstance(conf);
            long size = fs.getContentSummary(new Path(dir)).getLength();
            LogUtils.info("get hdfs size , path:" + dir + ", size:" + size);

            return size;
        } catch (Throwable e) {
            log.error("get hdfs size error!dir:"+dir, e);
            return -1;
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
