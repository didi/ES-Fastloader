package com.didichuxing.datachannel.arius.fastindex.remote.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.didichuxing.datachannel.arius.fastindex.es.config.IndexConfig;
import com.didichuxing.datachannel.arius.fastindex.es.config.mapping.TypeConfig;
import com.didichuxing.datachannel.arius.fastindex.es.config.mapping.TypeDefine;
import com.didichuxing.datachannel.arius.fastindex.es.config.mapping.TypeProperties;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import lombok.Data;
import org.apache.hadoop.mapreduce.JobContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
public class IndexInfo {
    private static final TypeDefine OBJECT_TYPE = new TypeDefine(JSON.parseObject("{\"type\":\"object\"}"));
    public static final String TEMPLATE_CONFIG = "indexConfig";

    private int shardNum;

    @JSONField(name = "indexConfig")
    private JSONObject setting;

    private String transformType;

    private long expanfactor;

    private String type;

    public static IndexInfo getIndexInfo(JobContext context) {
        String str = context.getConfiguration().get(TEMPLATE_CONFIG);
        return JSON.parseObject(str, IndexInfo.class);
    }


    @JSONField(serialize = false)
    public void check(TaskConfig taskConfig) throws Exception {
        if(expanfactor<=0) {
            throw new Exception("expanFactor error, expanfactor:" + expanfactor);
        }

        // 判断知否只有一个type，且type的名字是_doc
        IndexConfig indexConfig = new IndexConfig(setting);
        if (indexConfig.getMappings() == null) {
            return;
        }

        Map<String, TypeConfig> typeConfigMap = indexConfig.getMappings().getMapping();
        if(typeConfigMap==null) {
            return ;
        }

        Set<String> typeSet = typeConfigMap.keySet();
        typeSet.remove("_default_");

        if(typeSet.size()>1) {
            if(typeSet.contains(taskConfig.getHiveTable())) {
                type = taskConfig.getHiveTable();
            } else {
                throw new Exception("type size > 1, typeSet:" + typeSet);
            }

        } else {
            type = null;
            for (String type : typeSet) {
                this.type = type;
            }
            if (type == null || type.trim().length() == 0) {
                type = "type";
            }
        }
    }



    @JSONField(serialize = false)
    public Map<String, String> getTypeMap() throws Exception {
        Map<String, String> ret = new HashMap<>();

        // 获得es的一层type类型
        IndexConfig indexConfig = new IndexConfig(setting);
        if(indexConfig.getMappings()==null) {
            return ret;
        }

        Map<String, TypeConfig> typeConfigMap = indexConfig.getMappings().getMapping();
        if(typeConfigMap == null) {
            return ret;
        }

        for(String type : typeConfigMap.keySet()) {
            TypeConfig typeConfig = typeConfigMap.get(type);
            TypeProperties typeProperties = typeConfig.getProperties();
            if(typeProperties==null) {
                continue;
            }

            Map<String, TypeDefine> typeDefineMap = typeProperties.getJsonMap();
            for(String field : typeDefineMap.keySet()) {
                TypeDefine typeDefine = typeDefineMap.get(field);
                ret.put(field, typeDefine.getType());
            }


            Set<String> objectTypes = typeProperties.getPropertyMap().keySet();
            for(String field: objectTypes) {
                ret.put(field, "object");
            }
        }

        return ret;
    }
}
