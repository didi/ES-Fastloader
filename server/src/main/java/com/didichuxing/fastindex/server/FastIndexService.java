package com.didichuxing.fastindex.server;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.didichuxing.fastindex.common.es.IndexConfig;
import com.didichuxing.fastindex.common.es.catshard.ESIndicesSearchShardsResponse;
import com.didichuxing.fastindex.common.es.catshard.item.ESNode;
import com.didichuxing.fastindex.common.es.catshard.item.ESShard;
import com.didichuxing.fastindex.common.es.mapping.MappingConfig;
import com.didichuxing.fastindex.common.es.mapping.TypeConfig;
import com.didichuxing.fastindex.common.es.mapping.TypeDefine;
import com.didichuxing.fastindex.common.po.*;
import com.didichuxing.fastindex.dao.*;
import com.didichuxing.fastindex.utils.SizeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;


@Slf4j
@Service("fastIndexService")
public class FastIndexService {
    @Autowired
    private FastIndexLoadDataDao fastIndexLoadDataDao;

    @Autowired
    private FastIndexMappingEsDao fastIndexMappingEsDao;

    @Autowired
    private FastIndexOpIndexDao fastIndexOpIndexDao;

    @Autowired
    private FastIndexTaskMetricDao fastIndexTaskMetricDao;

    @Autowired
    private FastIndexTemplateConfigDao fastIndexTemplateConfigDao;

    @Autowired
    private IndexOpDao indexOpDao;

    public IndexFastIndexInfo getIndexConfig(String template, long time, boolean highES, long hdfsSize) throws Exception {
        String indexName = getIndexName(template, time);

        // 获得索引配置
        IndexConfig indexConfig = getIndexConfig(indexName);
        // 如果索引不存在，则创建索引
        if(indexConfig==null) {
            indexOpDao.createNewIndex(indexName);

            indexConfig = getIndexConfig(indexName);
            if(indexConfig==null) {
                throw new Exception("create index fail, index:" + indexName);
            }
        }

        // 确定reducer任务使用的索引的setting
        Map<String, String> settings = indexConfig.getSettings();
        long shardNum = Long.valueOf(settings.get("index.number_of_shards"));

        settings.put("index.number_of_shards", "1");
        settings.put("index.number_of_replicas", "0");
        settings.remove("index.routing.allocation.include.rack");
        settings.put("index.refresh_interval", "-1");
        settings.put("index.merge.scheduler.max_thread_count", "1");

        if(highES) {
            settings.remove("index.blocks.write");
            settings.remove("index.creation_date");
            settings.remove("index.provided_name");
            settings.remove("index.uuid");
            settings.remove("index.version.created");
            settings.remove("index.group.factor");
            settings.remove("index.group.name");
            settings.remove("index.template");
        }

        indexConfig.setSettings(settings);

        int expanFactor = decideExpanFactor(shardNum, hdfsSize);

        IndexFastIndexInfo indexFastIndexInfo = new IndexFastIndexInfo();
        indexFastIndexInfo.setIndexConfig(indexConfig);
        indexFastIndexInfo.setShardNum(shardNum*expanFactor);
        indexFastIndexInfo.setExpanfactor(expanFactor);

        // 使用特殊配置
        FastIndexTemplateConfigPo templateConfigPo = fastIndexTemplateConfigDao.getByName(template);
        if(templateConfigPo != null) {
            indexFastIndexInfo.setTransformType(templateConfigPo.getTransformType());
            if(templateConfigPo.getExpanfactor()>0) {
                indexFastIndexInfo.setExpanfactor(templateConfigPo.getExpanfactor());
                indexFastIndexInfo.setShardNum(shardNum * templateConfigPo.getExpanfactor());
            }
        }

        // 将索引配置成不可写, 失败重试
        indexOpDao.updateSetting(indexName, "blocks.write", "true");
        return indexFastIndexInfo;
    }

    /*
     * hdfsDir:/user/fe/data/es_fastload/bigPassenger/2019/04/24/es
     */
    public void startLoadData(String template, long time, String hdfsDir, int expanFactor, String hdfsUser, String hdfsPasswd, String srcTag) throws Exception {
        String indexName = getIndexName(template, time);

        // 判断索引是否完成
        if (isFinish(srcTag, indexName)) {
            throw new Exception("index is finished, indexName:" + indexName);
        }

        // 将副本数改为0个，如果副本数目>0,当前架构下，会有问题，只会迁移一个副本的数据
        indexOpDao.updateSetting(indexName, "number_of_replicas", "0");

        // 将索引配置成不可以rebalance
        indexOpDao.updateSetting(indexName, "routing.rebalance.enable", "none");

        // 产生新的mapping
        MappingConfig mappingConfig = mergeMapping(srcTag, template, time);
        if (mappingConfig != null) {
            Map<String, TypeConfig> mappings = mappingConfig.getMapping();
            for (String type : mappings.keySet()) {
                TypeConfig typeConfig = mappings.get(type);
                indexOpDao.updateMapping(indexName, type, typeConfig);
            }
        }

        // 获得各个shard所在的节点ip
        ESIndicesSearchShardsResponse resp = indexOpDao.getSearchShard(indexName);
        if (resp == null) {
            throw new Exception("get null ESIndicesSearchShardsResponse, template:" + template + ", time:" + time + ", index:" + indexName);
        }

        // 计算hdfsShard和ESShard的映射关系
        Map<Long/*esShard*/, List<Integer/*hdfsShard*/>> shardMap = new HashMap<>();
        int esShardNum = resp.getShards().size();
        int hdfsShardNum = expanFactor * esShardNum;

        for (int hdfsShard = 0; hdfsShard < hdfsShardNum; hdfsShard++) {
            long esShard = hdfsShard % esShardNum;

            if (!shardMap.containsKey(esShard)) {
                shardMap.put(esShard, new ArrayList<>());
            }

            shardMap.get(esShard).add(hdfsShard);
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

        for (List<ESShard> les : resp.getShards()) {
            for (ESShard es : les) {
                FastIndexLoadDataPo fastIndexLoadDataPo = new FastIndexLoadDataPo();

                fastIndexLoadDataPo.setTemplateName(template);
                fastIndexLoadDataPo.setIndexName(indexName);
                fastIndexLoadDataPo.setSrcTag(srcTag);
                fastIndexLoadDataPo.setIndexUUID(uuid);
                fastIndexLoadDataPo.setShardNum(es.getShard());

                // 默认9200端口
                fastIndexLoadDataPo.setPort(9200);
                fastIndexLoadDataPo.setHostName(nodeMap.get(es.getNode()).getName());
                fastIndexLoadDataPo.setHdfsShards(StringUtils.join(shardMap.get(es.getShard()), ","));
                fastIndexLoadDataPo.setHdfsUser(hdfsUser);
                fastIndexLoadDataPo.setHdfsPassword(hdfsPasswd);
                fastIndexLoadDataPo.setHdfsSrcDir(getHdfsPath(hdfsDir));
                fastIndexLoadDataPo.setEsDstDir(getEsPathFor661(uuid, es.getShard()));
                fastIndexLoadDataPo.setStart(false);
                fastIndexLoadDataPo.setFinish(false);
                fastIndexLoadDataPos.add(fastIndexLoadDataPo);
            }
        }

        // 将索引配置成不可写, 失败重试
        indexOpDao.updateSetting(indexName, "blocks.write", "true");

        // 写入任务数据，会有定时任务job执行各个任务
        fastIndexLoadDataDao.batchInsert(fastIndexLoadDataPos);
    }


    // 各个reducer任务提交统计信息
    public void submitMetric(String srcTag, String template, long time, long shardNum, JSONObject metric) throws Exception {
        String indexName = getIndexName(template, time);

        List<FastIndexTaskMetricPo> pos = new ArrayList<>();
        FastIndexTaskMetricPo po = new FastIndexTaskMetricPo();
        po.setSrcTag(srcTag);
        po.setIndexName(indexName);
        po.setAddTime(System.currentTimeMillis());
        po.setShardNum(shardNum);
        po.setMetrics(metric);
        pos.add(po);

        fastIndexTaskMetricDao.batchInsert(pos);
    }

    // mr结束的时候，获得整个任务的统计信息
    public JSONObject getAllMetrics(String srcTag, String template, long time) throws Exception {
        String indexName = getIndexName(template, time);

        List<FastIndexTaskMetricPo> pos = fastIndexTaskMetricDao.getByindexName(indexName);
        JSONObject ret = new JSONObject();
        for(FastIndexTaskMetricPo po : pos) {
            if(equalTag(srcTag, po.getSrcTag())) {
                ret.put(po.getShardNum()+"", po.getMetrics());
            }
        }

        return ret;
    }

    // 各个reducer任务提交mapping
    public void sumitMapping(String srcTag, String template, long time, long shardNum, JSONObject mapping) throws Exception {
        String indexName = getIndexName(template, time);

        List<FastIndexMappingPo> pos = new ArrayList<>();
        FastIndexMappingPo po = new FastIndexMappingPo();
        po.setSrcTag(srcTag);
        po.setIndexName(indexName);
        po.setAddTime(System.currentTimeMillis());
        po.setShardNum(shardNum);
        po.setMapping(JSON.toJSONString(mapping));
        pos.add(po);

        fastIndexMappingEsDao.batchInsert(pos);
    }

    // 合并reducer提交的mapping
    public MappingConfig mergeMapping(String srcTag, String template, long time) throws Exception {
        String indexName = getIndexName(template, time);

        List<FastIndexMappingPo> pos = fastIndexMappingEsDao.getByindexName(srcTag, indexName);
        if (pos == null) {
            return null;
        }

        // 检查是否有字段定义不一致的情况
        Map<String, TypeDefine> tdm = new HashMap<>();
        Map<String, Long> sm = new HashMap<>();
        for (FastIndexMappingPo po : pos) {
            MappingConfig mc = new MappingConfig(JSONObject.parseObject(po.getMapping()));

            Map<String, Map<String, TypeDefine>> m = mc.getTypeDefines();
            for (String type : m.keySet()) {
                for (String field : m.get(type).keySet()) {
                    String key = type + "." + field;
                    TypeDefine td = m.get(type).get(field);

                    if (!tdm.containsKey(key)) {
                        tdm.put(key, td);
                        sm.put(key, po.getShardNum());
                    } else {

                        if (!tdm.get(key).equals(td)) {
                            throw new Exception("field not match, index:" + indexName +
                                    ", shard1:" + po.getShardNum() +
                                    ", shard2:" + sm.get(key) +
                                    ", type:" + type +
                                    ", field:" + field);
                        }
                    }
                }
            }
        }

        // 合并字段
        MappingConfig mappingConfig = null;
        for (FastIndexMappingPo po : pos) {
            MappingConfig mc = new MappingConfig(JSONObject.parseObject(po.getMapping()));
            if (mappingConfig == null) {
                mappingConfig = mc;
            } else {
                mappingConfig.merge(mc);
            }
        }

        return mappingConfig;
    }

    public void removeFinishTag(String srcTag, String template, long time) throws Exception {
        String indexName = getIndexName(template, time);

        if(!isFinish(srcTag, indexName)) {
            return;
        }

        FastIndexOpIndexPo opIndexPo = new FastIndexOpIndexPo();
        opIndexPo.setIndexName(indexName);
        opIndexPo.setSrcTag(srcTag);
        String key = opIndexPo.getKey();

        fastIndexOpIndexDao.delete(key);
    }

    public boolean isFinish(String srcTag, String template, long time) throws Exception {
        String indexName = getIndexName(template, time);
        return isFinish(srcTag, indexName);
    }

    private boolean isFinish(String srcTag, String indexName) {
        List<FastIndexOpIndexPo> list = fastIndexOpIndexDao.getFinishedByIndexName(indexName);
        if(list==null || list.size()==0) {
            return false;
        }

        for(FastIndexOpIndexPo po : list) {
            if(!equalTag(po.getSrcTag(), srcTag)) {
                continue;
            }

            if(po.isFinish()) {
                return true;
            }
        }

        return false;
    }

    private String getHdfsPath(String rootPath) {
        if(rootPath.endsWith("/")) {
            rootPath = rootPath.substring(0, rootPath.length()-1);
        }

        return rootPath;
    }

    private static final String ES_PATH_FORMAT_661 = "/data1/es/nodes/0/indices/%s/%d";
    private String getEsPathFor661(String uuid, long shardId) {
        return String.format(ES_PATH_FORMAT_661, uuid, shardId);
    }

    private static final String WORK_DIR_FORMAT = "/data1/es/fastIndex/%s_shard%d";
    public static String getWorkDir(String index, long shardNum) {
        return String.format(WORK_DIR_FORMAT, index, shardNum);
    }

    private static final String DATE_FORMAT = "_yyyy-MM-dd";
    private String getIndexName(String template, long time) {
        SimpleDateFormat sdf = new SimpleDateFormat(template+DATE_FORMAT);
        return sdf.format(time);
    }

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


    private boolean equalTag(String tag1, String tag2) {
        if (tag1 == null) {
            if (tag2 == null) {
                return true;
            } else {
                return false;
            }
        } else {
            return tag1.equals(tag2);
        }
    }

    public static int EXPANFACTOR = 20;
    public int decideExpanFactor(long shardNum, long hdfsSize) {
        if(hdfsSize<=0) {
            return EXPANFACTOR;
        }

        long _1G = SizeUtil.getUnitSize("1g");

        // hdfs和es数据大小为 1:20
        long esSize = hdfsSize*20;

        // 单个reducer最多处理1g数据
        long reducerNum = esSize/_1G;

        int expanFactor = (int) (reducerNum/shardNum) + 1;

        if(expanFactor>EXPANFACTOR) {
            expanFactor = EXPANFACTOR;
        }

        return expanFactor;
    }
}
