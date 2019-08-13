package com.didi.bigdata.mr2es.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * 标签默认值工具
 */
@Slf4j
public class TagDefaultValueTools {

    public static Map<String, String[]> getTagDefaultValue(Configuration conf) {
        Map<String, String[]> resultMap = Maps.newHashMap();
        int retry = 0;
        while (retry < 3) {
            String url = getDefaultValueUrl(conf);
            log.info("post url is:{}, invoke:{}", url, retry);
            try {
                String res = HttpUtil.post(url);
                log.info("getTagDefaultValue response content:{}", res);
                JSONObject jsonArray = JSONObject.parseObject(res);
                if (!"10000".equals(jsonArray.getString("status"))) {
                    log.error("getTagDefaultValue fail!result:{}", res);
                } else {
                    JSONArray tags = jsonArray.getJSONArray("value");
                    log.info("tag default value result size : " + tags.size());
                    for (int i = 0; i < tags.size(); i++) {
                        String[] data = {tags.getJSONObject(i)
                                .getString("data_type"),
                                tags.getJSONObject(i)
                                        .getString("default_value")};
                        resultMap.put(tags.getJSONObject(i)
                                .getString("tag_name"), data);
                    }
                }
            } catch (Exception e) {
                log.error("post url error!url:{}", url);
            }
            try {
                if (MapUtils.isEmpty(resultMap)) {
                    retry++;
                    Thread.sleep(5 * 1000);
                } else {
                    break;
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
        return resultMap;
    }

    /**
     * 获取标签默认值url
     *
     * @param conf
     * @return
     */
    private static String getDefaultValueUrl(Configuration conf) {
        String userType = conf.get(Constants.KEY_USER_TYPE);
        log.info("user type is {}", userType);
        return Constants.TAG_DEFAULT_VALUE_URL + userType;
    }

}
