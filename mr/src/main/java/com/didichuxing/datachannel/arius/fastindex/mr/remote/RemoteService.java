package com.didichuxing.datachannel.arius.fastindex.mr.remote;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.didichuxing.datachannel.arius.fastindex.mr.metrics.TaskMetrics;
import com.didichuxing.datachannel.arius.fastindex.mr.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.HttpUtil;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.LogUtils;
import com.didichuxing.datachannel.arius.fastindex.server.server.FastIndexService;
import com.didichuxing.datachannel.arius.fastindex.server.server.IndexFastIndexInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.didichuxing.datachannel.arius.fastindex.mr.utils.HttpUtil.doHttpWithRetry;

@Slf4j
public class RemoteService {
    private static String SRC_TAG;
    public static String HOST ="127,0.0.1:8080";

    public static void setHost(String srcTag) {
        SRC_TAG = srcTag;
        LogUtils.info("host:" + HOST + "srcTag:" + SRC_TAG);
    }

    /*
     * 获得索引的配置信息，访问server的fastindex/getIndexInfo.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.getIndexInfo()函数
     */
    private static final String GET_TEMPLATE_PATH = "fastindex/getIndexInfo.do";
    public static IndexInfo getIndexConfig(String template, long time, long hdfsSize) {
        try {
            IndexFastIndexInfo indexFastIndexInfo = FastIndexService.getIndexConfig(template, time, hdfsSize);
            return JSON.parseObject(JSON.toJSONString(indexFastIndexInfo), IndexInfo.class);
        } catch (Throwable t) {
            throw new RuntimeException("getIndexConfig getException", t);
        }
    }


    /*
     * 触发数据加载流程，访问server的fastindex/startLoadData.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.startLoadData()函数
     */
    private static final String START_LOAD_PATH = "fastindex/startLoadData.do";
    public static String startLoadData(String template, long time, String hdfsDir, long expanfactor, String user, String passwd) {
        try {
            FastIndexService.startLoadData(template, time, hdfsDir, (int) expanfactor, user, passwd, SRC_TAG);
            return "ok";
        } catch (Throwable t) {
            throw new RuntimeException("startLoadData getException", t);
        }
    }

    /*
     * 提交mapping函数，访问server的fastindex/submitMapping.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.submitMapping()函数
     */
    private static final String SUBMIT_MAPPING_PATH = "fastindex/submitMapping.do";
    public static void submitMapping(String template, long time, int reduceId, JSONObject mapping) {
        try {
            FastIndexService.sumitMapping(SRC_TAG, template, time, reduceId, mapping);
        } catch (Throwable t) {
            throw new RuntimeException("submitMapping getException", t);
        }
    }

    /*
     * 提交metric函数，访问server的fastindex/submitMetric.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.submitMetric()函数
     */
    private static final String SUBMIT_METRIC_PATH = "fastindex/submitMetric.do";
    public static void submitMetric(String template, long time, int reduceId, TaskMetrics metric) {
        try {
            FastIndexService.submitMetric(SRC_TAG, template, time, reduceId, (JSONObject) JSONObject.toJSON(metric));
        } catch (Throwable t) {
            throw new RuntimeException("submitMapping getException", t);
        }
    }

    /*
     * 获得metric函数，访问server的fastindex/getMetricByIndex.do
     * 对应代码，server模块中的com.didichuxing.fastindex.controller.getMetricByIndex()函数
     */
    private static final String GET_METRIC_PATH = "fastindex/getMetricByIndex.do";
    public static Map<String, TaskMetrics> getMetric(String template, long time) {
        try {
            JSONObject obj = FastIndexService.getAllMetrics(SRC_TAG, template, time);
            return JSON.parseObject(obj.toJSONString(),new TypeReference<HashMap<String,TaskMetrics>>() {});
        } catch (Throwable t) {
            throw new RuntimeException("submitMapping getException", t);
        }
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
