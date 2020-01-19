package com.didichuxing.fastindex.dao;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.didichuxing.fastindex.common.po.BasePo;
import com.didichuxing.fastindex.utils.HttpUtil;

import java.util.List;
import java.util.logging.Logger;

public class BaseEsDao {
    protected static final Logger logger = Logger.getLogger(BaseEsDao.class.getName());
    protected static final String TYPE_NAME = "type";

    private String ip = "127.0.0.1";
    private int port = 9300;

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


        String result = HttpUtil.doHttpWithRetry(url, null, content, HttpUtil.HttpType.PUT, false, 5);
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
    }


    /**
     * 查询
     */
    protected String query(String indexName, String dsl) {
        String url =  getUrlPrefix() + "/" + indexName.trim() + "/_search";

        return HttpUtil.doHttpWithRetry(url,null,  dsl, HttpUtil.HttpType.GET, false, 5);
    }

    /**
     * 删除一个key
     */
    public void delete(String indexName, String typeName, String key) {
        String url =  getUrlPrefix() + "/" + indexName.trim() + "/" + typeName + "/" + key;
        HttpUtil.doHttpWithRetry(url,null,  null, HttpUtil.HttpType.DELETE, false, 5);
    }



    protected String getUrlPrefix() {
        return "http://" + ip + ":" + port;
    }


}

