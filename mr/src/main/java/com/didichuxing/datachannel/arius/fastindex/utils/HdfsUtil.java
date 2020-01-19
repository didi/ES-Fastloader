package com.didichuxing.datachannel.arius.fastindex.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * Created by WangZhuang on 2019/2/26
 */
@Slf4j
public class HdfsUtil {

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
