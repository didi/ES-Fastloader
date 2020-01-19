package com.didichuxing.datachannel.arius.fastindex.utils;

import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.es.config.IndexConfig;
import com.didichuxing.datachannel.arius.fastindex.es.config.mapping.MappingConfig;
import com.didichuxing.datachannel.arius.fastindex.es.config.mapping.TypeConfig;
import com.didichuxing.datachannel.arius.fastindex.es.config.mapping.TypeDefine;


import java.util.*;

public class MappingUtils {

    // 简化mapping，只处理property部分, 只提取type的内容
    public static JSONObject simple(JSONObject mappingJSON) throws Exception {
        MappingConfig dstMapping = new MappingConfig(mappingJSON);
        dstMapping.removeDefault();

        Map<String, TypeConfig> typeConfigMap = dstMapping.getMapping();
        for (String type : typeConfigMap.keySet()) {
            typeConfigMap.get(type).getNotUsedMap().clear();
        }

        return dstMapping.toJson();
    }


    public static JSONObject diffMapping(IndexConfig srcIndexConfig, JSONObject dstMappingJSON, String type) throws Exception {
        MappingConfig srcMapping = srcIndexConfig.getMappings();
        MappingConfig dstMapping = new MappingConfig(dstMappingJSON);

        Map<String, TypeConfig> srcTypeMap = srcMapping.getMapping();
        Map<String, TypeConfig> dstTypeMap = dstMapping.getMapping();

        if(type==null) {
            return null;
        }

        if(!srcTypeMap.containsKey(type) && !dstTypeMap.containsKey(type)) {
            return null;
        }

        if(srcTypeMap.containsKey(type) && !dstTypeMap.containsKey(type)) {
            return null;
        }

        if(!srcTypeMap.containsKey(type) && dstTypeMap.containsKey(type)) {
            // 只返回type的定义
            MappingConfig ret = new MappingConfig();
            ret.getMapping().put(type, dstTypeMap.get(type));
            return ret.toJson();
        }

        Map<String, TypeDefine> srcMap = srcTypeMap.get(type).getTypeDefine();
        Map<String, TypeDefine> dstMap = dstTypeMap.get(type).getTypeDefine();

        Map<String, TypeDefine> diffMap = new HashMap<>();
        for (String fieldName : dstMap.keySet()) {
            if (!srcMap.containsKey(fieldName)) {
                diffMap.put(fieldName, dstMap.get(fieldName));
            }
        }

        if (diffMap.size() == 0) {
            return null;
        }

        MappingConfig ret = new MappingConfig();
        for (String field : diffMap.keySet()) {
            List<String> fieldList = new ArrayList<>();
            for (String fieldName : field.split("\\.")) {
                fieldList.add(fieldName);
            }

            ret.addFields(type, fieldList, diffMap.get(field));
        }

        return ret.toJson();
    }
}
