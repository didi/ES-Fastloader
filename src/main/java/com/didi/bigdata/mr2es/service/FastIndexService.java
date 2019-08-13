package com.didi.bigdata.mr2es.service;

import com.alibaba.fastjson.JSONObject;
import com.didi.bigdata.mr2es.alarm.AlarmUtil;
import com.didi.bigdata.mr2es.utils.Constants;
import com.didi.bigdata.mr2es.utils.HttpUtil;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WangZhuang on 2019/4/30
 */
@Slf4j
public class FastIndexService {

    private static String URL_PREFIX = "http://127.0.0.1:30366/fastindex/";
    private static FastIndexService service = new FastIndexService();

    public static FastIndexService getInstance() {
        return service;
    }

    /**
     * 判断es方数据是否构建完成
     * @param template
     * @param time
     * @return
     */
    public boolean isFinishes(String template, long time) {
        StringBuilder sb = new StringBuilder();
        sb.append(URL_PREFIX);
        sb.append("isFinished.do?");
        sb.append("template=" + template);
        sb.append("&time=" + time);
        String url = sb.toString();
        String res = HttpUtil.get(url);
        log.info("is finished res is:{}", res);
        if (StringUtils.isNotBlank(res)) {
            JSONObject jsonObject = JSONObject.parseObject(res);
            if (jsonObject.getBooleanValue("data")) {
                log.info("load data success!");
                return true;
            }
        }
        return false;
    }

    /**
     * 开始load 数据
     * @param template
     * @param time
     * @param hdfsDir
     * @return
     */
    public boolean startLoadData(String template, long time,
                                 String hdfsDir) {
        log.info("start load data params template:{},time:{},hdfs:{}",
                template, time, hdfsDir);
        StringBuilder sb = new StringBuilder();
        sb.append(URL_PREFIX);
        sb.append("startLoadData.do?");
        sb.append("template=" + template);
        sb.append("&time=" + time);
        sb.append("&hdfsDir=" + hdfsDir);
        sb.append("&expanFactor=10");
        String url = sb.toString();
        log.info("start load data url is:{}", url);
        String res = HttpUtil.get(url);
        log.info("start load data res is:{}", res);
        if (StringUtils.isNotBlank(res)) {
            JSONObject jsonObject = JSONObject.parseObject(res);
            if (jsonObject.getIntValue("code") == 0) {
                log.info("notice es start load data success!");
                return true;
            }
        }
        log.info("notice es start load data error,please check!");
        return false;
    }

    /**
     * 提交mapping
     *
     * @param template
     * @param time
     * @param shardNum
     * @param mapping
     */
    public void submitMapping(String template, long time,
                              int shardNum, String mapping) {
        String url = URL_PREFIX + "submitMapping.do";
        Map<String, Object> map = Maps.newHashMap();
        map.put("template", template);
        map.put("time", time);
        map.put("shardNum", shardNum);
        map.put("mapping", mapping);
        String res = HttpUtil.postJsonEntity(url, new JSONObject(map));
        log.info("submit mapping res is:{}", res);
    }

    /**
     * 获取index config
     *
     * @param template
     * @param time
     * @return
     */
    public Map<String, String> getIndex(String template, long time) {
        int tryCount = 3;
        for (int i = 0; i < tryCount; i++) {
            try {
                Map<String, String> resMap = Maps.newHashMap();
                StringBuilder sb = new StringBuilder();
                sb.append(URL_PREFIX);
                sb.append("getIndexInfo.do?");
                sb.append("template=" + template);
                sb.append("&time=" + time);
                sb.append("&expanFactor=10");
                String url = sb.toString();
                log.info("get mapping url is:{}", url);
                String res = HttpUtil.get(url);
                if (StringUtils.isNotBlank(res)) {
                    JSONObject jsonObject = JSONObject.parseObject(res);
                    if (jsonObject.getIntValue("code") == 0) {
                        JSONObject data = jsonObject.getJSONObject("data");
                        resMap.put(Constants.KEY_INDEX_CONFIG, data.getString("indexConfig"));
                        resMap.put(Constants.KEY_REDUCE_NUM, data.getString("shardNum"));
                        return resMap;
                    }
                }
            } catch (Throwable e) {
                log.error("get mapping error!", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return new HashMap<>();
    }

}
