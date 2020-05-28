package com.didichuxing.datachannel.arius.fastindex.server.job;


import com.alibaba.fastjson.JSON;
import com.didichuxing.datachannel.arius.fastindex.server.common.po.FastIndexLoadDataPo;
import com.didichuxing.datachannel.arius.fastindex.server.common.po.FastIndexOpIndexPo;
import com.didichuxing.datachannel.arius.fastindex.server.dao.FastIndexLoadDataDao;
import com.didichuxing.datachannel.arius.fastindex.server.dao.FastIndexOpIndexDao;
import com.didichuxing.datachannel.arius.fastindex.server.dao.IndexOpDao;
import com.didichuxing.datachannel.arius.fastindex.server.job.limiter.CocurrentLimiter;
import com.didichuxing.datachannel.arius.fastindex.server.job.limiter.ShardLimiter;
import com.didichuxing.datachannel.arius.fastindex.server.server.FastIndexService;
import com.didichuxing.datachannel.arius.fastindex.server.utils.ZeusUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;


// 负责处理各个shard的数据搬迁
@Slf4j
public class FastIndexLoadDataCollector {
    // 1.提高性能，合并单个节点的执行过程, 一个任务的带宽为50MB/s
    // 2.多次重试失败，则放弃，并告警
    public static void handleJobTask(FastIndexLoadDataParam param) {
        log.info("class=FastIndexLoadDataCollector ||method=handleJobTask||params={}", JSON.toJSONString(param));

        CocurrentLimiter limiter = new CocurrentLimiter(param.getMaxCocurrent(), param.getNodeMaxCocurrent());
        ShardLimiter shardLimiter = new ShardLimiter();

        List<FastIndexLoadDataPo> poList = FastIndexLoadDataDao.getAll();
        if(poList==null || poList.size()==0) {
            return;
        }
        log.info("fastIndexLoadDataCollector get poList size:" + poList.size());

        long current = System.currentTimeMillis();
        // 判断完成是否完成po
        for(FastIndexLoadDataPo po : poList) {
            if(po.isStart() && !po.isFinish()) {
                if (!isFinish(po)) {
                    if(ZeusUtils.isDone(po.getZeusTaskId()) ||
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
            String srcTag = null;

            for(FastIndexLoadDataPo po : allIndexMap.get(opKey)) {
                templateName = po.getTemplateName();
                indexName = po.getIndexName();
                srcTag = po.getSrcTag();
                if(!po.isFinish()) {
                    isFinish = false;
                }
            }

            if(isFinish) {
                log.info("fast index open cluster:" + clusterName + ",index:" + indexName);

                FastIndexOpIndexPo opIndexPo = new FastIndexOpIndexPo();
                opIndexPo.setTemplateName(templateName);
                opIndexPo.setIndexName(indexName);
                opIndexPo.setSrcTag(srcTag);
                opIndexPo.setFinish(false);
                opIndexPos.add(opIndexPo);
            }
        }


        // 写入结果
        if(opIndexPos.size()>0) {
            FastIndexOpIndexDao.batchInsert(opIndexPos);
        }
        FastIndexLoadDataDao.batchInsert(poList);
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

    private static boolean isFinish(FastIndexLoadDataPo po) {
        long taskId = po.getZeusTaskId();
        if(taskId<=0) {
            log.warn("wrong task id, taskId:" + taskId + ", common:" + JSON.toJSONString(po));
        }

        try {
            String resp = ZeusUtils.getStdOut(taskId);
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

    private static final long HIGH_ES_ZEUS_TEMPLATE_ID = 1246;
    private static boolean startLoadData(FastIndexLoadDataPo po) {
        long taskId;

        try {
            log.info("start load data, common:" + JSON.toJSONString(po));

            List<String> params = new ArrayList<>();
            long zeusTemplaeId = 0;

            zeusTemplaeId = HIGH_ES_ZEUS_TEMPLATE_ID;
            params.add(po.getHdfsSrcDir());
            params.add(po.getIndexName());
            params.add(po.getIndexUUID());
            params.add("" + po.getShardNum());
            params.add(FastIndexService.getWorkDir(po.getIndexName(), po.getShardNum()));
            params.add(po.getRedcueIds());
            params.add("_uid");     // 固定使用es内部的_uid;
            params.add(po.getHdfsUser());
            params.add(po.getHdfsPassword());
            params.add("" + po.getPort());

            // 这里需要指定对用的用户名
            String user = "root";

            long timeout = 1200;

            taskId = ZeusUtils.startTask(zeusTemplaeId, user, po.getHostName(), timeout, params);

            if (taskId <= 0) {
                log.warn("zeus get wrong taskId, taskId:" + taskId + ", common:" + JSON.toJSONString(po));
                return false;
            }

        } catch (Throwable t) {
            log.warn("start load Data get error, common:" + JSON.toJSONString(po), t);
            return false;
        }

        po.setZeusTaskId(taskId);
        po.setStart(true);
        po.setStartTime(System.currentTimeMillis());
        po.setRunCount(po.getRunCount()+1);
        return true;
    }
}
