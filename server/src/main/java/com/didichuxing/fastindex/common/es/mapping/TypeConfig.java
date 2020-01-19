package com.didichuxing.fastindex.common.es.mapping;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeConfig {
    private Map<String, Object> notUsedMap = new HashMap<>();

    private TypeProperties      properties = null;

    public TypeConfig() {
    }

    public TypeConfig(JSONObject root) throws Exception {
        if (root == null) {
            throw new Exception("root is null");
        }

        for (String key : root.keySet()) {
            if (key.equalsIgnoreCase(TypeProperties.PROPERTIES_STR)) {
                properties = new TypeProperties(root.getJSONObject(key));
            } else {
                notUsedMap.put(key, root.get(key));
            }
        }
    }

    public void addProperties(Map<String, Object> m) {
        notUsedMap.putAll(m);
    }

    public void addField(String field, TypeDefine define) {
        if (properties == null) {
            properties = new TypeProperties();
        }

        properties.addField(field, define);
    }

    public void addFields(List<String> fields, TypeDefine define) {
        if (properties == null) {
            properties = new TypeProperties();
        }

        properties.addFields(fields, define);
    }

    public void delField(List<String> fields) {
        if (properties == null) {
            properties = new TypeProperties();
        }

        properties.delFields(fields);
    }



    public void deleteField(String fieldName) {
        if (properties != null) {
            properties.deleteField(fieldName);
        }
    }

    public boolean isFieldExists(String fieldName) {
        if (properties != null) {
            Map<String, TypeDefine> fieldNameMap = properties.getTypeDefine();
            for (String field : fieldNameMap.keySet()) {
                if (field.equals(fieldName)) {
                    return true;
                }
            }
        }

        return false;
    }

    public JSONObject toJson() {
        JSONObject root = new JSONObject();

        for (String key : notUsedMap.keySet()) {
            root.put(key, notUsedMap.get(key));
        }

        if (properties != null) {
            root.put(TypeProperties.PROPERTIES_STR, properties.toJson());
        }

        return root;
    }

    public JSONObject toJson(ESVersion version) {
        JSONObject root = new JSONObject();

        for (String key : notUsedMap.keySet()) {
            boolean highVersionDiscard = ESVersion.ES651.equals(
                version) && ("dynamic_templates".equalsIgnoreCase(key)
                    || "dynamic_date_formats".equalsIgnoreCase(key)
                    || "_ttl".equalsIgnoreCase(key)
                    || "_all".equalsIgnoreCase(key));
            if (highVersionDiscard) {
                // 高版本不再需要以上字段
                continue;
            }

            root.put(key, notUsedMap.get(key));
        }

        if (properties != null) {
            root.put(TypeProperties.PROPERTIES_STR, properties.toJson(version));
        }

        return root;
    }

    public Map<String, TypeDefine> getTypeDefine() {
        if (properties != null) {
            return properties.getTypeDefine();
        } else {
            return new HashMap<>();
        }
    }

    public boolean isEmpty() {
        if (notUsedMap != null && notUsedMap.size() > 0) {
            return false;
        }

        if (properties != null && !properties.isEmpty()) {
            return false;
        }

        return true;
    }

    /**
     * 获取原生类型定义
     *
     * @return
     */
    public Map<String, TypeDefine> getTypeDefineRaw() {
        if (properties != null) {
            return properties.getJsonMap();
        } else {
            return new HashMap<>();
        }
    }

    // tm的field类型覆盖当前类型
    public void merge(TypeConfig tm) {
        if (properties != null) {
            properties.merge(tm.properties);
        }
    }

    public Map<String, Object> getNotUsedMap() {
        return notUsedMap;
    }

    public void setNotUsedMap(Map<String, Object> notUsedMap) {
        this.notUsedMap = notUsedMap;
    }

    public TypeProperties getProperties() {
        return properties;
    }

    public void setProperties(TypeProperties properties) {
        this.properties = properties;
    }
}
