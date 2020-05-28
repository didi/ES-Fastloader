package com.didichuxing.datachannel.arius.fastindex.mr.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.didichuxing.datachannel.arius.fastindex.mr.metrics.TaskMetrics;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.HttpUtil;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;

/* 封装对ES的操作 */
@Slf4j
public class ESClient {
    private TaskMetrics taskMetrics = new TaskMetrics();    // 统计信息
    public Integer port;                                    // ES的端口
    public String index;                                    // 操作的索引名
    public String type;                                     // 写入的type名
    private boolean isFirstError = true;                    // 用于只打印第一次错误的信息

    public ESClient(Integer port, String index, String type) {
        this.port = port;
        this.index = index;
        this.type = type;

        LogUtils.info("esclient port:" + port + ", index:" + index + ", type:" + type);
    }

    public TaskMetrics getTaskMetrics() {
        return taskMetrics;
    }


    /* 修改集群配置 */
    public void putClusterSetting(JSONObject param) {
        JSONObject body = new JSONObject();
        body.put("persistent", param);


        String url = getUrlPrefix() + "/_cluster/settings";
        String result = HttpUtil.doHttpWithRetry(url,null,  body.toJSONString(), HttpUtil.HttpType.PUT, false, 5);
        LogUtils.info("esclient put cluster setting ret:" + result);
    }

    /* 创建Index */
    public void createNewIndex(JSONObject setting) throws Exception {
        String url = getUrlPrefix() + "/" + index.trim();
        String ret = HttpUtil.doHttpWithRetry(url, null, setting.toJSONString(), HttpUtil.HttpType.PUT, false, 5);
        LogUtils.info("esclient create index, setting:" + setting.toJSONString() + ", ret:" + ret);
    }

    /* 修改indexsetting */
    public void putSetting(String key, String value) {
        JSONObject body = new JSONObject();
        body.put(key, value);

        String url = getUrlPrefix() + "/" + index.trim() + "/_settings";
        String result = HttpUtil.doHttpWithRetry(url,null,  body.toJSONString(), HttpUtil.HttpType.PUT, false, 5);
        LogUtils.info("esclient put setting ret:" + result);
    }

    /* 获得Index mapping */
    public JSONObject getMapping() throws Exception {
        String url = getUrlPrefix() + "/" + index.trim() + "/_mapping";
        String result = HttpUtil.doHttpWithRetry(url,null,  null, HttpUtil.HttpType.GET, false, 5);
        LogUtils.info("esclient get mapping ret:" + result);

        JSONObject obj = JSONObject.parseObject(result);
        return obj.getJSONObject(index).getJSONObject("mappings");
    }

    /* 获得Index setting和mapping */
    public void getSetting() throws Exception {
        String url = getUrlPrefix() + "/" + index.trim();
        String result = HttpUtil.doHttpWithRetry(url,null,  null, HttpUtil.HttpType.GET, false, 5);
        LogUtils.info("esclient get setting ret:" + result);
    }


    /* forceMerge索引，将segment数目降低为1个 */
    public void forceMerge() throws Exception {
        String url = getUrlPrefix() + "/" + index.trim() + "/_forcemerge";
        JSONObject param = new JSONObject();
        param.put("max_num_segments", 1);
        param.put("flush", true);

        String str = HttpUtil.doHttpWithRetry(url, param, null, HttpUtil.HttpType.POST, false, 5);
        LogUtils.info("esclient force merge ret:" + str);
    }

    /* flush索引 */
    private static final String SHARD_STR = "_shards";
    public boolean flush() throws Exception {
        String url = getUrlPrefix() + "/" + index.trim() + "/_flush";
        String result = HttpUtil.doHttpWithRetry(url, null, "{}", HttpUtil.HttpType.POST, false, 5);
        LogUtils.info("esclient flush ret:" + result);

        JSONObject obj = JSONObject.parseObject(result);

        if(obj.containsKey(SHARD_STR)) {
            int fail = obj.getJSONObject(SHARD_STR).getInteger("failed");
            if(fail==0) {
                return true;
            }
        }

        return false;
    }

    /* refresh索引 */
    public boolean refresh() throws Exception {
        String url = getUrlPrefix() + "/" + index.trim() + "/_refresh";
        String result = HttpUtil.doHttpWithRetry(url, null, "{}", HttpUtil.HttpType.POST, false, 5);
        LogUtils.info("esclient refresh ret:" + result);

        JSONObject obj = JSONObject.parseObject(result);

        if(obj.containsKey(SHARD_STR)) {
            int fail = obj.getJSONObject(SHARD_STR).getInteger("failed");
            if(fail==0) {
                return true;
            }
        }

        return false;
    }

    /* 多线程写入数据 */
    public void indexNodes(List<IndexNode> nodes) {
        long start = System.currentTimeMillis();

        String url = getUrlPrefix() + "/" + index.trim() + "/" + type.trim() + "/_bulk";

        StringBuilder sb = new StringBuilder();
        for(IndexNode node : nodes) {
            if (node.key != null && node.key.getBytes(StandardCharsets.UTF_8).length >= 512) {
                taskMetrics.addErrorRecords(1);
                continue;
            }

            JSONObject keyObj = new JSONObject();
            JSONObject idObj = new JSONObject();
            idObj.put("_id", node.key);
            keyObj.put("index", idObj);

            sb.append(keyObj.toJSONString());
            sb.append("\n");
            sb.append(JSON.toJSONString(node.data, SerializerFeature.WriteMapNullValue));
            sb.append("\n");
        }
        String content = sb.toString();
        if(content.length()==0) {
            return;
        }


        taskMetrics.addReadBytes(content.length());
        taskMetrics.addReadRecords(nodes.size());

        String result = HttpUtil.doHttpWithRetry(url, null, content, HttpUtil.HttpType.PUT, false, 5);
        String str = result;
        if(result.length()>60) {
            str = result.substring(0, 50);
        }

        // TODO 解析JSON，细化处理
        if(str.contains("\"errors\":true") || str.contains("root_cause")) {
            if(isFirstError) {
                LogUtils.info("index data error, data:" + content);
                LogUtils.info("index data error, ret:" + result);

                isFirstError=false;
            }

            // 统计错误条数 TODO 遍历数据，精细化
            taskMetrics.addErrorRecords(nodes.size());

        } else {
            taskMetrics.addWriteRecords(nodes.size());
        }

        long cost = System.currentTimeMillis()-start;
        if(cost>10000) {
            LogUtils.info("cost to much, cost:" + cost + "ms");
        }
    }

    private String getUrlPrefix() {
        return "http://127.0.0.1:" + port;
    }

    public static class IndexNode {
        public String key;
        public JSONObject data;
    }
}
