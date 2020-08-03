package com.didichuxing.datachannel.arius.fastindex.es;

import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;

import java.util.ArrayList;
import java.util.List;

/* 负责多线程写入ES数据 */
public class EsWriter {
    private ESClient esClient;
    private Integer batchSize = 500;        // 单次写入的数据个数
    private Integer threadPoolSize = 4;     // 线程个数
    private List<ESClient.IndexNode> nodeList = new ArrayList<>();  // 当前缓存的ES数据
    private volatile boolean isStop = false;
    private Object lock = new Object();
    private List<Thread> threadPool = new ArrayList<>();


    public EsWriter(ESClient esClient, Integer batchSize, Integer threadPoolSize) {
        this.esClient = esClient;

        if(batchSize!=null) {
            this.batchSize = batchSize;
        }

        if(threadPoolSize!=null) {
            this.threadPoolSize = threadPoolSize;
        }

        LogUtils.info("batch size:" + batchSize + ", thread pool size:" + threadPoolSize);


        for(int i=0; i<this.threadPoolSize; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while(!isStop) {
                        try {
                            // 获得数据，并写入
                            List<ESClient.IndexNode> data = getNodeList();
                            if (data != null) {
                                esClient.indexNodes(data);
                            } else {
                                // 数据没有准备好，sleep
                                try {
                                    Thread.sleep(1000);
                                } catch (Throwable t) {
                                    LogUtils.error("sleep error", t);
                                }
                            }
                        } catch (Throwable t) {
                            LogUtils.error("write node error", t);
                        }
                    }
                    LogUtils.info("thread finish, name:" + Thread.currentThread().getName());
                }
            });
            thread.setDaemon(true);
            thread.setName("fastIndexWriterES_" + i);
            thread.start();

            threadPool.add(thread);
        }
    }

    public void bulk(String key, JSONObject data) {
        ESClient.IndexNode node = new ESClient.IndexNode();
        node.key = key;
        node.data = data;

        putNode(node);
    }

    /* 如果大于batchSize 则等待 */
    private void putNode(ESClient.IndexNode indexNode) {
        while(true) {
            synchronized (lock) {
                if (nodeList.size() < batchSize) {
                    nodeList.add(indexNode);
                    return;
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LogUtils.error("sleep error", e);
            }
        }
    }

    private List<ESClient.IndexNode> getNodeList() {
        synchronized (lock) {
            if (nodeList.size() >= batchSize) {
                List ret = nodeList;
                nodeList = new ArrayList<>();
                return ret;
            } else {
                return null;
            }
        }
    }


    public void finish() {
        isStop = true;
        for(Thread thread : threadPool) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                LogUtils.error("wait for thread finish error", e);
            }
        }

        synchronized (lock) {
            if(nodeList.size()>0) {
                esClient.indexNodes(nodeList);
            }
        }
    }
}
