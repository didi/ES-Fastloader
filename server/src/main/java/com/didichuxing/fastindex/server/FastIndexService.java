package com.didichuxing.fastindex.server;


import com.didichuxing.fastindex.common.es.IndexConfig;
import com.didichuxing.fastindex.common.es.catshard.ESIndicesSearchShardsResponse;
import com.didichuxing.fastindex.common.es.catshard.item.ESNode;
import com.didichuxing.fastindex.common.es.catshard.item.ESShard;
import com.didichuxing.fastindex.common.po.FastIndexLoadDataPo;
import com.didichuxing.fastindex.common.po.FastIndexOpIndexPo;
import com.didichuxing.fastindex.dao.FastIndexLoadDataDao;
import com.didichuxing.fastindex.dao.FastIndexOpIndexDao;
import com.didichuxing.fastindex.dao.IndexOpDao;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Service("fastIndexService")
public class FastIndexService {
    public static int EXPANFACTOR = 5;

    @Autowired
    private FastIndexLoadDataDao fastIndexLoadDataDao;

    @Autowired
    private FastIndexOpIndexDao fastIndexOpIndexDao;

    @Autowired
    private IndexOpDao indexOpDao;

    /*
     * 获得索引的配置信息
     * @param template 模板名
     * @param time  时间分区
     * @param hdfsSize hive数据大小，用于确定expandFactor
     *
     */
    public IndexFastIndexInfo getIndexConfig(String template, long time) throws Exception {
        String indexName = getIndexName(template, time);

        // 获得索引配置
        IndexConfig indexConfig = getIndexConfig(indexName);

        // 如果索引不存在，则创建索引
        if (indexConfig == null) {
            indexOpDao.createNewIndex(indexName);

            indexConfig = getIndexConfig(indexName);
            if (indexConfig == null) {
                throw new Exception("create index fail, index:" + indexName);
            }
        }

        // 确定reducer任务使用的索引的setting
        IndexFastIndexInfo indexFastIndexInfo = new IndexFastIndexInfo();

        Map<String, String> settings = indexConfig.getSettings();
        settings.put("index.number_of_shards", "1");
        settings.put("index.number_of_replicas", "0");
        settings.put("index.refresh_interval", "-1");
        settings.put("index.merge.scheduler.max_thread_count", "1");
        settings.remove("index.routing.allocation.include.rack");
        settings.remove("index.blocks.write");
        settings.remove("index.creation_date");
        settings.remove("index.provided_name");
        settings.remove("index.uuid");
        settings.remove("index.version.created");
        settings.remove("index.group.factor");
        settings.remove("index.group.name");
        settings.remove("index.template");
        indexConfig.setSettings(settings);
        indexFastIndexInfo.setIndexConfig(indexConfig);

        long shardNum = Long.valueOf(settings.get("index.number_of_shards"));
        indexFastIndexInfo.setReducerNum(shardNum * EXPANFACTOR);

        return indexFastIndexInfo;
    }

    /*
     * 触发数据加载任务
     * @param tempalte 模板名
     * @param time 时间分区
     * @param hdfsDir 存放lucene文件的hdfs路径
     * @param expanFactor getIndexConfig返回给mr任务
     * @param hdfsUser hdfs用户名
     * @param hdfsPasswd hdfs密码
     */
    public void startLoadData(String template, long time, String hdfsDir, int reducerNum, String hdfsUser, String hdfsPasswd, String esWorkDir) throws Exception {
        String indexName = getIndexName(template, time);

        // 判断索引是否完成
        if (isFinish(indexName)) {
            throw new Exception("index is finished, indexName:" + indexName);
        }

        // 将副本数改为0个，如果副本数目>0,当前架构下，会有问题，只会迁移一个副本的数据
        indexOpDao.updateSetting(indexName, "number_of_replicas", "0");
        // 将索引配置成不可以rebalance
        indexOpDao.updateSetting(indexName, "routing.rebalance.enable", "none");

        // 获得各个shard所在的节点ip
        ESIndicesSearchShardsResponse resp = indexOpDao.getSearchShard(indexName);
        if (resp == null) {
            throw new Exception("get null ESIndicesSearchShardsResponse, template:" + template + ", time:" + time + ", index:" + indexName);
        }

        // 计算hdfsShard和ESShard的映射关系
        Map<Long/*esShard*/, List<Integer/*hdfsShard*/>> shardMap = new HashMap<>();
        int esShardNum = resp.getShards().size();
        for (int reduceId = 0; reduceId < reducerNum; reduceId++) {
            long esShard = reduceId% esShardNum;

            if (!shardMap.containsKey(esShard)) {
                shardMap.put(esShard, new ArrayList<>());
            }

            shardMap.get(esShard).add(reduceId);
        }

        // 获得缩影uuid
        IndexConfig indexConfig = getIndexConfig(indexName);
        Map<String, String> settings = indexConfig.getSettings();
        String uuid = settings.get("index.uuid");

        // 产生数据搬迁任务
        List<FastIndexLoadDataPo> fastIndexLoadDataPos = new ArrayList<>();
        Map<String, ESNode> nodeMap = resp.getNodes();
        for (List<ESShard> les : resp.getShards()) {
            if (les == null || les.size() == 0) {
                throw new Exception("es node is failed, for index:" + indexName);
            }
        }

        // 构建各个shard的数据加载任务
        for (List<ESShard> les : resp.getShards()) {
            for (ESShard es : les) {
                FastIndexLoadDataPo fastIndexLoadDataPo = new FastIndexLoadDataPo();

                fastIndexLoadDataPo.setCreateTime(System.currentTimeMillis());
                fastIndexLoadDataPo.setTemplateName(template);
                fastIndexLoadDataPo.setIndexName(indexName);
                fastIndexLoadDataPo.setIndexUUID(uuid);
                fastIndexLoadDataPo.setShardNum(es.getShard());

                // 默认9200端口
                fastIndexLoadDataPo.setPort(9500);
                fastIndexLoadDataPo.setHostName(nodeMap.get(es.getNode()).getName());
                fastIndexLoadDataPo.setRedcueIds(StringUtils.join(shardMap.get(es.getShard()), ","));
                fastIndexLoadDataPo.setHdfsUser(hdfsUser);
                fastIndexLoadDataPo.setHdfsPassword(hdfsPasswd);
                fastIndexLoadDataPo.setHdfsSrcDir(getHdfsPath(hdfsDir));
                fastIndexLoadDataPo.setEsWordDir(esWorkDir);
                fastIndexLoadDataPo.setStart(false);
                fastIndexLoadDataPo.setFinish(false);

                fastIndexLoadDataPos.add(fastIndexLoadDataPo);
            }
        }

        // 写入任务数据，会有定时任务job执行各个任务
        // 对应的job任务是com.didichuxing.fastindex.job.FastIndexLoadDataCollector
        fastIndexLoadDataDao.batchInsert(fastIndexLoadDataPos);
    }


    /*
     * 移除所有结束标记
     * @param template 模板名
     * @param time 分区时间
     */
    public void removeFinishTag(String template, long time) throws Exception {
        String indexName = getIndexName(template, time);

        if(!isFinish(indexName)) {
            return;
        }

        FastIndexOpIndexPo opIndexPo = new FastIndexOpIndexPo();
        opIndexPo.setIndexName(indexName);
        String key = opIndexPo.getKey();

        fastIndexOpIndexDao.delete(key);
    }

    /*
     * 判断加载任务是否完成
     * @param template 模板名
     * @param time 分区时间
     */
    public boolean isFinish(String template, long time) throws Exception {
        String indexName = getIndexName(template, time);
        return isFinish(indexName);
    }

    /*
     * 判断加载任务是否完成
     * @param template 模板名
     * @param time 分区时间
     */
    private boolean isFinish(String indexName) throws Exception {
        List<FastIndexOpIndexPo> list = fastIndexOpIndexDao.getFinishedByIndexName(indexName);
        if(list==null || list.size()==0) {
            return false;
        }

        for(FastIndexOpIndexPo po : list) {
            if(po.isFinish()) {
                return true;
            }
        }

        return false;
    }

    /* 去除路径末尾的/ */
    private String getHdfsPath(String rootPath) {
        if(rootPath.endsWith("/")) {
            rootPath = rootPath.substring(0, rootPath.length()-1);
        }

        return rootPath;
    }

    /* 获得索引名 */
//    private static final String DATE_FORMAT = "_yyyy-MM-dd";
    private static final String DATE_FORMAT = "_yyyyMMdd";
    private String getIndexName(String template, long time) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        return template + sdf.format(time);
    }

    /* 获得索引配置 */
    private IndexConfig getIndexConfig(String indexName) throws Exception {
        Map<String, IndexConfig> m = indexOpDao.getSetting(indexName);
        if(m==null) {
            return null;
        }

        IndexConfig indexConfig = null;
        for (String index : m.keySet()) {
            if (index.startsWith(indexName)) {
                indexConfig = m.get(index);
            }
        }

        return indexConfig;
    }

}
