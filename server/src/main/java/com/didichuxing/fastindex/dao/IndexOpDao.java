package com.didichuxing.fastindex.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.didichuxing.fastindex.common.es.TemplateConfig;
import com.didichuxing.fastindex.common.es.IndexConfig;
import com.didichuxing.fastindex.common.es.catshard.ESIndicesSearchShardsResponse;
import com.didichuxing.fastindex.common.es.mapping.TypeConfig;
import com.didichuxing.fastindex.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.didichuxing.fastindex.utils.HttpUtil.doHttp;


@Component
@Slf4j
public class IndexOpDao extends BaseEsDao {

    /* 获得Index mapping */
    public Map<String, IndexConfig> getSetting(String indexName) throws Exception {
        String url = getUrlPrefix() + "/" + indexName.trim();
        String result = doHttp(url,null,  null, HttpUtil.HttpType.GET);
        if(result==null) {
            throw new Exception("get setting get null, indexName:" + indexName);
        }

        log.info("esclient get setting ret:" + result);

        Map<String, IndexConfig> indexConfigMap = new HashMap<>();
        JSONObject root = JSONObject.parseObject(result);
        for(String name : root.keySet()) {
            indexConfigMap.put(name, new IndexConfig(root.getJSONObject(name)));
        }
        return indexConfigMap;
    }


    /* 修改indexsetting */
    public void updateSetting(String indexName, String key, String value) throws Exception {
        JSONObject body = new JSONObject();
        body.put(key, value);

        String url = getUrlPrefix() + "/" + indexName.trim() + "/_settings";
        String result = doHttp(url,null,  body.toJSONString(), HttpUtil.HttpType.PUT);
        if(result==null) {
            throw new Exception("update setting get null, indexName:" + indexName);
        }
        log.info("esclient put setting ret:" + result);
    }

    public void createNewIndex(String indexName) throws Exception {
        String url = getUrlPrefix() + "/" + indexName;
        String ret = doHttp(url, null, null, HttpUtil.HttpType.PUT);
        if(ret==null) {
            throw new Exception("create new index get null, indexName:" + indexName);
        }
        log.info("esclient create index, name:" + indexName + ", ret:" + ret);
    }

    public void updateMapping(String indexName, String typeName, TypeConfig typeConfig) throws Exception {
        String url = getUrlPrefix() + "/" + indexName + "/_mapping/" + typeName;
        String ret = doHttp(url, null, typeConfig.toJson().toJSONString(), HttpUtil.HttpType.PUT);
        if(ret==null) {
            throw new Exception("update mapping get null, indexName:" + indexName);
        }
        log.info("esclient update index mapping, name:" + indexName + ", ret:" + ret);
    }

    public ESIndicesSearchShardsResponse getSearchShard(String indexName) throws Exception {
        String url = getUrlPrefix() + "/" + indexName.trim()  + "/_search_shards";
        String ret = doHttp(url, null, null, HttpUtil.HttpType.GET);
        if(ret==null) {
            throw new Exception("get search shard get null, indexName:" + indexName);
        }
        log.info("esclient getSearchShard, name:" + indexName + ", ret:" + ret);
        return JSON.parseObject(ret, ESIndicesSearchShardsResponse.class);
    }

    public TemplateConfig getTemplate(String templateName) throws Exception {
        String url = getUrlPrefix() + "/_template/" + templateName.trim();
        String ret = doHttp(url, null, null, HttpUtil.HttpType.GET);
        if(ret==null) {
            throw new Exception("get template shard get null, templateName:" + templateName);
        }
        log.info("esclient getTemplate, name:" + templateName + ", ret:" + ret);

        JSONObject root = JSONObject.parseObject(ret);
        for(String name : root.keySet()) {
            return new TemplateConfig(root.getJSONObject(name));
        }

        return null;
    }
}
