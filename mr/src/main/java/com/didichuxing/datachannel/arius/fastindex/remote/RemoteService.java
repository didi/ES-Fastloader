package com.didichuxing.datachannel.arius.fastindex.remote;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
    private static String HOST_STR;
    private static String URL_FORMAT_STR = "http://%s/%s";

    public static void setHost(String host) {
        HOST_STR = host;
        LogUtils.info("host:" + HOST_STR);
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
        param.put("hdfsUser", user);
        param.put("hdfsPasswd", passwd);

        String url = String.format(URL_FORMAT_STR, HOST_STR, START_LOAD_PATH);
        LogUtils.info("send http, url:" + url + ", param:" + param.toJSONString());
        String ret = doHttpWithRetry(url,  param, null, HttpUtil.HttpType.GET, true, 30);
        LogUtils.info("start load path ret:" + ret);
        return  ret;
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

        String url = String.format(URL_FORMAT_STR, HOST_STR, REMOVE_FINISH_TAG_PATH);
        String result = doHttpWithRetry(url, param,null, HttpUtil.HttpType.GET, true, 5);
        LogUtils.info("remove Finish Tag ret:" + result);
    }
}
