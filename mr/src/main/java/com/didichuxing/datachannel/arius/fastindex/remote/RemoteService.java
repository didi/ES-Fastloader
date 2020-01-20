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


    /*
     * 获得索引的配置信息，访问server的fastindex/getIndexInfo.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.getIndexInfo()函数
     */
    private static final String GET_TEMPLATE_PATH = "fastindex/getIndexInfo.do";
    public static IndexInfo getIndexConfig(String template, long time, long hdfsSize) {
        JSONObject param = new JSONObject();
        param.put("template", template);
        param.put("time", time);
        param.put("highES", true);
        param.put("hdfsSize", hdfsSize);


        String url = String.format(URL_FORMAT_STR, HOST_STR, GET_TEMPLATE_PATH);
        LogUtils.info("send http, url:" + url + ", param:" + param.toJSONString());
        String res = doHttpWithRetry(url, param,null, HttpUtil.HttpType.GET, true, 5);
        LogUtils.info("getIndexConfig response:" + res);
        if (res == null) {
            return null;
        }

        return JSON.parseObject(res, IndexInfo.class);
    }


    /*
     * 触发数据加载流程，访问server的fastindex/startLoadData.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.startLoadData()函数
     */
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

    /*
     * 提交mapping函数，访问server的fastindex/submitMapping.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.submitMapping()函数
     */
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

    /*
     * 提交metric函数，访问server的fastindex/submitMetric.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.submitMetric()函数
     */
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

    /*
     * 获得metric函数，访问server的fastindex/getMetricByIndex.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.getMetricByIndex()函数
     */
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

    /*
     * 判断任务是否已经结束，访问server的fastindex/isFinished.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.isFinished()函数
     */
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

    /*
     * 去除任务结束标识，访问server的fastindex/removeFinishTag.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.removeFinishTag()函数
     */
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
