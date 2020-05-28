package com.didichuxing.datachannel.arius.fastindex.server.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.server.common.es.TemplateConfig;
import com.didichuxing.datachannel.arius.fastindex.server.common.es.IndexConfig;
import com.didichuxing.datachannel.arius.fastindex.server.common.es.catshard.ESIndicesSearchShardsResponse;
import com.didichuxing.datachannel.arius.fastindex.server.common.es.mapping.TypeConfig;
import com.didichuxing.datachannel.arius.fastindex.server.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static com.didichuxing.datachannel.arius.fastindex.server.utils.HttpUtil.doHttpWithRetry;


@Slf4j
public class IndexOpDao extends BaseEsDao {

    /* 获得Index mapping */
    public static Map<String, IndexConfig> getSetting(String indexName) throws Exception {
        String url = getUrlPrefix() + "/" + indexName.trim();
        String result = doHttpWithRetry(url,null,  null, HttpUtil.HttpType.GET, false, 5);
        log.info("esclient get setting ret:" + result);

        Map<String, IndexConfig> indexConfigMap = new HashMap<>();
        JSONObject root = JSONObject.parseObject(result);
        for(String name : root.keySet()) {
            indexConfigMap.put(name, new IndexConfig(root.getJSONObject(name)));
        }
        return indexConfigMap;
    }


    /* 修改indexsetting */
    public static void updateSetting(String indexName, String key, String value) {
        JSONObject body = new JSONObject();
        body.put(key, value);

        String url = getUrlPrefix() + "/" + indexName.trim() + "/_settings";
        String result = doHttpWithRetry(url,null,  body.toJSONString(), HttpUtil.HttpType.PUT, false, 5);
        log.info("esclient put setting ret:" + result);
    }

    public static void createNewIndex(String indexName) throws Exception {
        String url = getUrlPrefix() + "/" + indexName;
        String ret = doHttpWithRetry(url, null, null, HttpUtil.HttpType.PUT, false, 5);
        log.info("esclient create index, name:" + indexName + ", ret:" + ret);
    }

    public static void updateMapping(String indexName, String typeName, TypeConfig typeConfig) {
        String url = getUrlPrefix() + "/" + indexName + "/_mapping/" + typeName;
        String ret = doHttpWithRetry(url, null, typeConfig.toJson().toJSONString(), HttpUtil.HttpType.PUT, false, 5);
        log.info("esclient update index mapping, name:" + indexName + ", ret:" + ret);
    }

    public static ESIndicesSearchShardsResponse getSearchShard(String indexName) {
        String url = getUrlPrefix() + "/" + indexName.trim()  + "/_search_shards";
        String ret = doHttpWithRetry(url, null, null, HttpUtil.HttpType.GET, false, 5);
        log.info("esclient getSearchShard, name:" + indexName + ", ret:" + ret);

        return JSON.parseObject(ret, ESIndicesSearchShardsResponse.class);
    }



    public static TemplateConfig getTemplate(String templateName) throws Exception {
        String url = getUrlPrefix() + "/_template/" + templateName.trim();
        String ret = doHttpWithRetry(url, null, null, HttpUtil.HttpType.GET, false, 5);
        log.info("esclient getTemplate, name:" + templateName + ", ret:" + ret);


        JSONObject root = JSONObject.parseObject(ret);
        for(String name : root.keySet()) {
            return new TemplateConfig(root.getJSONObject(name));
        }

        return null;
    }
}
