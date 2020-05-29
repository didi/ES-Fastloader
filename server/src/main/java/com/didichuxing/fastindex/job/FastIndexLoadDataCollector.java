package com.didichuxing.fastindex.job;


import com.alibaba.fastjson.JSON;
import com.didichuxing.fastindex.common.po.FastIndexLoadDataPo;
import com.didichuxing.fastindex.common.po.FastIndexOpIndexPo;
import com.didichuxing.fastindex.dao.FastIndexLoadDataDao;
import com.didichuxing.fastindex.dao.FastIndexOpIndexDao;
import com.didichuxing.fastindex.dao.IndexOpDao;
import com.didichuxing.fastindex.job.limiter.CocurrentLimiter;
import com.didichuxing.fastindex.job.limiter.ShardLimiter;
import com.didichuxing.fastindex.remoteshell.RemoteShell;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;


// 负责处理各个shard的数据搬迁
@Slf4j
@Component
public class FastIndexLoadDataCollector {
    @Autowired
    private FastIndexLoadDataDao fastIndexLoadDataDao;

    @Autowired
    private FastIndexOpIndexDao fastIndexOpIndexDao;

    @Autowired
    private IndexOpDao clusterClientPool;

    @Autowired
    private RemoteShell remoteShell;

    // 1.提高性能，合并单个节点的执行过程, 一个任务的带宽为50MB/s
    // 2.多次重试失败，则放弃，并告警
    public void handleJobTask() throws Exception {
        FastIndexLoadDataParam param = new FastIndexLoadDataParam();

        CocurrentLimiter limiter = new CocurrentLimiter(param.getMaxCocurrent(), param.getNodeMaxCocurrent());
        ShardLimiter shardLimiter = new ShardLimiter();

        List<FastIndexLoadDataPo> poList = fastIndexLoadDataDao.getAll();
        if(poList==null || poList.size()==0) {
            return;
        }
        log.info("fastIndexLoadDataCollector get poList size:" + poList.size());

        long current = System.currentTimeMillis();
        // 判断完成是否完成po
        for(FastIndexLoadDataPo po : poList) {
            if(po.isStart() && !po.isFinish()) {
                if (!isFinish(po)) {
                    if(remoteShell.isShellDone(po.getShellTaskId()) ||
                            current-po.getStartTime() > param.getWaitTimeout()) {
                        // zeus任务已经完成，或者超时 则重试
                        po.setStart(false);
                        po.setStartTime(0);

                    } else {
                        limiter.add(po.getHostName());
                        shardLimiter.add(po.getIndexName(), po.getShardNum());
                    }
                }
            }
        }

        // 分配执行任务任务
        for(FastIndexLoadDataPo po : poList) {
            if(po.isStart() || po.isFinish()) {
                continue;
            }

            if(limiter.tryOne(po.getHostName()) &&
                    shardLimiter.tryOne(po.getIndexName(), po.getShardNum())) {
                if(startLoadData(po)) {
                    limiter.add(po.getHostName());
                    shardLimiter.add(po.getIndexName(), po.getShardNum());
                }
            }
        }

        // open操作
        List<FastIndexOpIndexPo> opIndexPos = new ArrayList<>();
        Map<String, List<FastIndexLoadDataPo>> allIndexMap = new HashMap<>();
        for(FastIndexLoadDataPo po : poList) {
            String opKey = po.getOpKey();
            if(!allIndexMap.containsKey(opKey)) {
                allIndexMap.put(opKey, new ArrayList<>());
            }

            allIndexMap.get(opKey).add(po);
        }

        for(String opKey : allIndexMap.keySet()) {
            boolean isFinish = true;
            String clusterName = null;
            String templateName = null;
            String indexName = null;

            for(FastIndexLoadDataPo po : allIndexMap.get(opKey)) {
                templateName = po.getTemplateName();
                indexName = po.getIndexName();
                if(!po.isFinish()) {
                    isFinish = false;
                }
            }

            if(isFinish) {
                log.info("fast index open cluster:" + clusterName + ",index:" + indexName);

                FastIndexOpIndexPo opIndexPo = new FastIndexOpIndexPo();
                opIndexPo.setTemplateName(templateName);
                opIndexPo.setIndexName(indexName);
                opIndexPo.setFinish(false);
                opIndexPos.add(opIndexPo);
            }
        }


        // 写入结果
        if(opIndexPos.size()>0) {
            fastIndexOpIndexDao.batchInsert(opIndexPos);
        }
        fastIndexLoadDataDao.batchInsert(poList);
    }

    private static Set<String> shardSuccessTags = new HashSet<>();
    static {
        shardSuccessTags.add("loadhdfsOK");
        shardSuccessTags.add("tarOK");
        shardSuccessTags.add("mvindexOK");
    }

    private static Set<String> successTags = new HashSet<>();
    static {
        successTags.add("appendluceneOK");
        successTags.add("allOK");
    }

    private boolean isFinish(FastIndexLoadDataPo po) {
        long taskId = po.getShellTaskId();
        if(taskId<=0) {
            log.warn("wrong task id, taskId:" + taskId + ", common:" + JSON.toJSONString(po));
        }

        try {
            String resp = remoteShell.getShellOutput(taskId);
            for (String tag : successTags) {
                if (!resp.contains(tag)) {
                    return false;
                }
            }

            String hdfsShards = po.getRedcueIds();
            int num = hdfsShards.split(",").length;
            for (int i = 0; i < num; i++) {
                for (String tag : shardSuccessTags) {
                    if (!resp.contains(tag + i)) {
                        return false;
                    }
                }
            }


            log.info("common finish，common:" + JSON.toJSONString(po));
            po.setFinish(true);
            po.setFinishTime(System.currentTimeMillis());
            return true;
        } catch (Throwable t) {
            log.warn("task get std out getException, common:" + JSON.toJSONString(po), t);
            return false;
        }
    }

    private boolean startLoadData(FastIndexLoadDataPo po) {
        long taskId;

        try {
            log.info("start load data, common:" + JSON.toJSONString(po));

            List<String> params = new ArrayList<>();
            params.add(po.getHdfsSrcDir());
            params.add(po.getIndexName());
            params.add(po.getIndexUUID());
            params.add("" + po.getShardNum());
            params.add(getWorkDir(po.getEsWordDir(), po.getIndexName(), po.getShardNum()));
            params.add(po.getRedcueIds());
            params.add("_uid");     // 固定使用es内部的_uid;
//            params.add(po.getHdfsUser());
//            params.add(po.getHdfsPassword());
            params.add("prod_arius_es");
            params.add("VthoTKeeMxAVHLULuPYA90O1lPhN3kwN");
            params.add("" + po.getPort());

            // 这里需要指定对用的用户名
            taskId = remoteShell.startShell(po.getHostName(),  params);

            if (taskId <= 0) {
                log.warn("zeus get wrong taskId, taskId:" + taskId + ", common:" + JSON.toJSONString(po));
                return false;
            }

        } catch (Throwable t) {
            log.warn("start load Data get error, common:" + JSON.toJSONString(po), t);
            return false;
        }

        po.setShellTaskId(taskId);
        po.setStart(true);
        po.setStartTime(System.currentTimeMillis());
        po.setRunCount(po.getRunCount()+1);
        return true;
    }

    /* 获得数据加载脚本的工作路径 */
    private static final String WORK_DIR_FORMAT = "%s/%s_shard%d";
    public static String getWorkDir(String worddir, String index, long shardNum) {
        return String.format(WORK_DIR_FORMAT, worddir, index, shardNum);
    }
}
