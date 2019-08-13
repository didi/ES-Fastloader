package com.didi.bigdata.mr2es.utils;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.math.MathUtils;

/**
 *
 */
public class Ids {

  public static void main(String[] arg) {
      String id = getDocId();
      System.out.println("doc id is:" + id);
      System.out.println("shardId is:" + getShardId(id, 10));
  }

  public static String getDocId(){
    return Strings.randomBase64UUID();
  }

    /**
     * 基于es 2.0计算docId所属分片
     * @param docId
     * @param shardsNum
     * @return
     */
  public static int getShardId(String docId, int shardsNum) {
    int hash = Murmur3HashFunction.hash(docId);
    return MathUtils.mod(hash, shardsNum);
  }
}
