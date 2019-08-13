package com.didi.bigdata.mr2es.es;

import com.alibaba.fastjson.JSONObject;
import com.didi.bigdata.mr2es.utils.HttpUtil;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.Base64;
import java.util.List;
import java.util.Random;

/**
 * es 预search
 * Created by WangZhuang on 2019/6/24
 */
@Slf4j
public class ESSearch {

    private static final String TOKEN =
            "Basic " + Base64.getEncoder()
                    .encodeToString("appId:token".getBytes());
    private static List<String> dslList = Lists.newArrayList();

    static {
        //todo: dslList.add("dsl_statement");
    }

    public int getCount(String indexName) throws Exception {
        Random random = new Random();
        String dsl = dslList.get(random.nextInt(dslList.size()));
        int totalCount = 0;
        dsl = fromRule(dsl);
        String url = "http://127.0.0.1:8000/arius/" +
                indexName + "/indextype/_search";
        String res = HttpUtil.postEsByDsl(url, dsl, TOKEN);
        if (StringUtils.isNotBlank(res)) {
            JSONObject resJson = JSONObject.parseObject(res);
            JSONObject hits = resJson.getJSONObject("hits");
            if (hits != null) {
                totalCount = hits.getIntValue("total");
            }
        }
        return totalCount;
    }

    private String fromRule(String rule) {
        return String.format("{\n" +
                "  \"fields\" : [\"passenger_id\"],\n" +
                "  \"query\" : {\n" +
                "    \"filtered\" : {\n" +
                "      \"filter\" :     {\n" +
                "       \"bool\" :          {\n" +
                "       \"should\" :              [%s\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"sort\": [\n" +
                "    {\n" +
                "      \"_doc\": {\n" +
                "        \"order\": \"desc\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]" +
                "}", rule);
    }

}
