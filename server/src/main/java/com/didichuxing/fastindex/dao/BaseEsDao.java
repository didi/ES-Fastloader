package com.didichuxing.fastindex.dao;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.didichuxing.fastindex.common.po.BasePo;
import com.didichuxing.fastindex.utils.HttpUtil;

import java.util.List;
import java.util.logging.Logger;

public class BaseEsDao {
    protected static final Logger logger = Logger.getLogger(BaseEsDao.class.getName());
    protected static final String TYPE_NAME = "type";

    private String ip = "ecm-arius-dcdr-clientnode-sf-5838a-0.docker.ys";
    private int port = 9200;

    /**
     * 批量写入
     */
    protected boolean batchInsert(String indexName, String typeName, List<? extends BasePo> pos) {
        String url =  getUrlPrefix() + "/" + indexName.trim() + "/" + typeName.trim() + "/_bulk";

        StringBuilder sb = new StringBuilder();
        for (BasePo node : pos) {
            JSONObject keyObj = new JSONObject();
            JSONObject idObj = new JSONObject();
            idObj.put("_id", node.getKey());
            keyObj.put("index", idObj);

            sb.append(keyObj.toJSONString());
            sb.append("\n");
            sb.append(JSON.toJSONString(node, SerializerFeature.WriteMapNullValue));
            sb.append("\n");
        }
        String content = sb.toString();
        if (content.length() == 0) {
            return true;
        }


        String result = HttpUtil.doHttp(url, null, content, HttpUtil.HttpType.PUT);
        if(result!=null) {
            String str = result;
            if (result.length() > 60) {
                str = result.substring(0, 50);
            }

            // TODO 解析JSON，细化处理
            if (str.contains("\"errors\":true") || str.contains("root_cause")) {
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }


    /**
     * 查询
     */
    protected String query(String indexName, String dsl) throws Exception {
        String url =  getUrlPrefix() + "/" + indexName.trim() + "/_search";
        String ret = HttpUtil.doHttp(url,null,  dsl, HttpUtil.HttpType.GET);

        if (ret == null) {
            throw new Exception("query return null, indexName:" + indexName + ", dsl:" + dsl);
        }

        // 提取hit数据
        JSONObject obj = JSON.parseObject(ret);
        if(obj.getBoolean("timed_out")) {
            throw new Exception("query time out, ret:" + ret);
        }

        JSONObject shard = obj.getJSONObject("_shards");
        if(shard.getInteger("failed")>0) {
            throw new Exception("query have failed shard");
        }

        JSONArray array = obj.getJSONObject("hits").getJSONArray("hits");

        JSONArray a = new JSONArray();
        for(Object o : array) {
            JSONObject oj = (JSONObject) o;
            a.add(oj.getJSONObject("_source"));
        }

        return a.toJSONString();
    }

    /**
     * 删除一个key
     */
    public void delete(String indexName, String typeName, String key) {
        String url =  getUrlPrefix() + "/" + indexName.trim() + "/" + typeName + "/" + key;
        HttpUtil.doHttp(url,null,  null, HttpUtil.HttpType.DELETE);
    }



    protected String getUrlPrefix() {
        return "http://" + ip + ":" + port;
    }


}

