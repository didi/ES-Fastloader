package com.didichuxing.datachannel.arius.fastindex.remote;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.didichuxing.datachannel.arius.fastindex.metrics.TaskMetrics;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.utils.HttpUtil;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.didichuxing.datachannel.arius.fastindex.utils.HttpUtil.doHttpWithRetry;

@Slf4j
public class RemoteService {
    private static final String ONLINE_HOST_STR = "127.0.0.1:8080";
    private static final String PRE_HOST_STR = "127.0.0.1:8080";
    private static final String TEST_HOST_STR = "127.0.0.1:8080";

    private static String SRC_TAG;
    private static String HOST_STR;
    private static String URL_FORMAT_STR = "http://%s/%s";

    public static void setHost(EnvEnum env, String srcTag) {
        SRC_TAG = srcTag;
        switch (env) {
            case ONLINE:
                HOST_STR = ONLINE_HOST_STR;
                break;
            case PRE:
                HOST_STR = PRE_HOST_STR;
                break;
            default:
                HOST_STR = TEST_HOST_STR;
        }

        LogUtils.info("env:" + env.getCode() + ", host:" + HOST_STR + "srcTag:" + SRC_TAG);
    }


    private static final String IS_FASTINDEX_PATH= "fastindex/isFastIndex.do";
    public static boolean isFastIndex(String template) {
        JSONObject param = new JSONObject();
        param.put("template", template);

        String url = String.format(URL_FORMAT_STR, HOST_STR, IS_FASTINDEX_PATH);
        String result = doHttpWithRetry(url, param,null, HttpUtil.HttpType.GET, true, 5);
        LogUtils.info("is fast index ret:" + result);
        if (StringUtils.isBlank(result)) {
            return false;
        }

        return Boolean.valueOf(result);
    }


    private static final String GET_TEMPLATE_PATH = "fastindex/getIndexInfo.do";
    public static IndexInfo getTemplateConfig(String template, long time, long hdfsSize) {
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("highES", true);
        param.put("hdfsSize", hdfsSize);


        String url = String.format(URL_FORMAT_STR, HOST_STR, GET_TEMPLATE_PATH);
        LogUtils.info("send http, url:" + url + ", param:" + param.toJSONString());
        String res = doHttpWithRetry(url, param,null, HttpUtil.HttpType.GET, true, 5);
        LogUtils.info("getTemplateConfig response:" + res);
        if (res == null) {
            return null;
        }

        return JSON.parseObject(res, IndexInfo.class);
    }


    private static final String START_LOAD_PATH = "fastindex/startLoadData.do";
    public static String startLoadData(String template, long time, String hdfsDir, long expanfactor, String user, String passwd) {
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("hdfsDir", hdfsDir);
        param.put("expanFactor", expanfactor);
        param.put("highES", true);
        param.put("hdfsUser", user);
        param.put("hdfsPasswd", passwd);
        param.put("srcTag", SRC_TAG);

        String url = String.format(URL_FORMAT_STR, HOST_STR, START_LOAD_PATH);
        LogUtils.info("send http, url:" + url + ", param:" + param.toJSONString());
        String ret = doHttpWithRetry(url,  param, null, HttpUtil.HttpType.GET, true, 30);
        LogUtils.info("start load path ret:" + ret);
        return  ret;
    }

    private static final String SUBMIT_MAPPING_PATH = "fastindex/submitMapping.do";
    public static void submitMapping(String template, long time, int shardNum, JSONObject mapping) {
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("shardNum", shardNum);
        param.put("mapping", mapping);
        param.put("srcTag", SRC_TAG);

        String url = String.format(URL_FORMAT_STR, HOST_STR, SUBMIT_MAPPING_PATH);
        String res = doHttpWithRetry(url, null, param.toJSONString(), HttpUtil.HttpType.POST, true, 5);
        LogUtils.info("submitMapping res:" + res);
    }

    private static final String SUBMIT_METRIC_PATH = "fastindex/submitMetric.do";
    public static void submitMetric(String template, long time, int shardNum, TaskMetrics metric) {
        LogUtils.info("submit metric " + JSON.toJSONString(metric));
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("shardNum", shardNum);
        param.put("srcTag", SRC_TAG);
        param.put("metric", JSONObject.parseObject(JSON.toJSONString(metric)));

        String url = String.format(URL_FORMAT_STR, HOST_STR, SUBMIT_METRIC_PATH);
        String res = doHttpWithRetry(url, null, param.toJSONString(), HttpUtil.HttpType.POST, true, 5);
        LogUtils.info("submitMetric res:" + res);
    }

    private static final String GET_METRIC_PATH = "fastindex/getMetricByIndex.do";
    public static Map<String, TaskMetrics> getMetric(String template, long time) {
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("srcTag", SRC_TAG);

        String url = String.format(URL_FORMAT_STR, HOST_STR, GET_METRIC_PATH);
        String res = doHttpWithRetry(url, param, null, HttpUtil.HttpType.GET, true, 5);
        LogUtils.info("getMetric res:" + res);

        return JSON.parseObject(res ,new TypeReference<HashMap<String,TaskMetrics>>() {});
    }


    private static final String IS_FINISH_PATH= "fastindex/isFinished.do";
    public static boolean loadDataIsFinish(String template, long time) {
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("srcTag", SRC_TAG);

        String url = String.format(URL_FORMAT_STR, HOST_STR, IS_FINISH_PATH);
        String result = doHttpWithRetry(url, param,null, HttpUtil.HttpType.GET, true, 5);
        LogUtils.info("load data is finish ret:" + result);
        if (StringUtils.isBlank(result)) {
            return false;
        }

        return Boolean.valueOf(result);
    }

    private static final String REMOVE_FINISH_TAG_PATH = "fastindex/removeFinishTag.do";
    public static void removeFinishTag(String template, long time) {
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("srcTag", SRC_TAG);

        String url = String.format(URL_FORMAT_STR, HOST_STR, REMOVE_FINISH_TAG_PATH);
        String result = doHttpWithRetry(url, param,null, HttpUtil.HttpType.GET, true, 5);
        LogUtils.info("remove Finish Tag ret:" + result);
    }
}
