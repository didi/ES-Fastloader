package com.didichuxing.fastindex.dao;

import com.alibaba.fastjson.JSON;
import com.didichuxing.fastindex.common.po.FastIndexLoadDataPo;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FastIndexLoadDataDao extends BaseEsDao {
    /* 索引功能说明 */
    private static final String INDEX_NAME = "fast.index.loadata";
    /* mapping
    *
    * {
      "type" : {
        "properties" : {
          "clusterName" : {
            "type" : "keyword"
          },
          "esDstDir" : {
            "type" : "keyword"
          },
          "finish" : {
            "type" : "boolean"
          },
          "finishTime" : {
            "type" : "long"
          },
          "hdfsPassword" : {
            "type" : "keyword"
          },
          "hdfsShards" : {
            "type" : "keyword"
          },
          "hdfsSrcDir" : {
            "type" : "keyword"
          },
          "hdfsUser" : {
            "type" : "keyword"
          },
          "highES" : {
            "type" : "boolean"
          },
          "hostName" : {
            "type" : "keyword"
          },
          "indexName" : {
            "type" : "keyword"
          },
          "indexUUID" : {
            "type" : "keyword"
          },
          "key" : {
            "type" : "keyword"
          },
          "port" : {
            "type" : "long"
          },
          "runCount" : {
            "type" : "long"
          },
          "shardNum" : {
            "type" : "long"
          },
          "srcTag" : {
            "type" : "keyword"
          },
          "start" : {
            "type" : "boolean"
          },
          "startTime" : {
            "type" : "long"
          },
          "templateName" : {
            "type" : "keyword"
          },
          "zeusTaskId" : {
            "type" : "long"
          }
        }
      }
    }
    */


    public boolean batchInsert(List<FastIndexLoadDataPo> poList) {
        return batchInsert(INDEX_NAME, TYPE_NAME, poList);
    }

    public List<FastIndexLoadDataPo> getAll() {
        String dsl = "{" +
                "   \"size\": 10000\n" +
                " }";

        String source = query(INDEX_NAME, dsl);
        return JSON.parseArray(source, FastIndexLoadDataPo.class);
    }
}
