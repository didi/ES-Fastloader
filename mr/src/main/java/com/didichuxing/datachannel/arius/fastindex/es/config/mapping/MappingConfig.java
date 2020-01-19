package com.didichuxing.datachannel.arius.fastindex.es.config.mapping;

import com.alibaba.fastjson.JSONObject;

import java.util.*;

public class MappingConfig {
    private Map<String, TypeConfig> mapping = new HashMap<>();

    public MappingConfig() {}
    public MappingConfig(JSONObject root) throws Exception {
        if(root == null) {
            throw new Exception("root is null");
        }

        for(String key : root.keySet()) {
            mapping.put(key, new TypeConfig(root.getJSONObject(key)));
        }
    }


    public boolean isEmpty() {
        if(mapping.size()==0) {
            return true;
        }
        return false;
    }

    public Map<String, TypeConfig> getMapping() {
        return mapping;
    }


    public JSONObject toJson() {
        JSONObject root = new JSONObject();

        for (String key : mapping.keySet()) {
            root.put(key, mapping.get(key).toJson());
        }

        return root;
    }

    public JSONObject toJson(ESVersion version) {
        JSONObject root = new JSONObject();

        for(String key : mapping.keySet()) {
            root.put(key, mapping.get(key).toJson(version));
        }

        return root;
    }



    public void addField(String typeName, String field, TypeDefine typeDefine) {
        if(!mapping.containsKey(typeName)) {
            mapping.put(typeName, new TypeConfig());
        }

        mapping.get(typeName).addField(field, typeDefine);
    }

   public void addFields(String typeName, List<String> fields, TypeDefine typeDefine) {
       if(!mapping.containsKey(typeName)) {
            mapping.put(typeName, new TypeConfig());
        }

        mapping.get(typeName).addFields(fields, typeDefine);
   }

   public void delFields(List<String> fields) {
       for(String type : mapping.keySet()) {
           mapping.get(type).delField(fields);
       }
    }



    public void deleteField(String typeName, String fieldName) {
        if(!mapping.containsKey(typeName)) {
            return;
        }

        mapping.get(typeName).deleteField(fieldName);
    }

    /**
     * 判断字段是否存在
     *
     * @param fieldName
     * @return
     */
    public boolean isFieldExist(String fieldName) {
        for(String typeName : mapping.keySet()) {
            if (mapping.get(typeName).isFieldExists(fieldName)) {
                return true;
            }
        }

        return false;
    }

    public void addType(String typeName, Map<String, Object> pro) {
        if(!mapping.containsKey(typeName)) {
            mapping.put(typeName, new TypeConfig());
        }

        mapping.get(typeName).addProperties(pro);
    }



    public Map<String/*typeName*/, Map<String/*field*/, TypeDefine>> getTypeDefines() {
        Map<String, Map<String, TypeDefine>> ret = new HashMap<>();

        for(String type : mapping.keySet()) {
            Map m = mapping.get(type).getTypeDefine();
            if(m.size()>0) {
                ret.put(type, m);
            }
        }

        return ret;
    }

    /**
     * 获取原生字段定义
     *
     * @return
     */
    public Map<String/*typeName*/, Map<String/*field*/, TypeDefine>> getTypeDefinesRaw() {
        Map<String, Map<String, TypeDefine>> ret = new HashMap<>();

        for(String type : mapping.keySet()) {
            Map m = mapping.get(type).getTypeDefineRaw();
            if(m.size()>0) {
                ret.put(type, m);
            }
        }

        return ret;
    }


    public Map<String/*field*/, List<TypeDefine>> getTypes() {
        Map<String, List<TypeDefine>> ret = new HashMap<>();

        for(String type : mapping.keySet()) {
            Map<String, TypeDefine> m = mapping.get(type).getTypeDefine();

            for(String field : m.keySet()) {
                if(!ret.containsKey(field)) {
                    ret.put(field, new ArrayList<>());
                }

                ret.get(field).add(m.get(field));
            }
        }

        return ret;
    }

    // 判断mapping只是否有字段在多处使用不同的定义
    public Set<String> checkMapping() {
        Map<String, List<TypeDefine>> m = getTypes();

        Set<String> ret = new HashSet<>();
        for(String field : m.keySet()) {
            List<TypeDefine> l = m.get(field);
            if(l==null || l.size()<=1) {
                continue;
            }

            TypeDefine typeDefine = l.get(0);
            for(int i=1; i<l.size(); i++) {
                if(!typeDefine.equals(l.get(i))) {
                    ret.add(field);
                    break;
                }
            }
        }

        return ret;
    }

    public Set<String> diffTypeDefine(MappingConfig mappings) {
        Set<String> ret = new HashSet<>();
        if (mappings == null) {
            return ret;
        }

        Map<String, List<TypeDefine>> m1 = this.getTypes();
        Map<String, List<TypeDefine>> m2 = mappings.getTypes();


        for (String field : m1.keySet()) {
            if (!m2.containsKey(field)) {
                continue;
            }

            List<TypeDefine> l1 = m1.get(field);
            List<TypeDefine> l2 = m2.get(field);
            for (TypeDefine td1 : l1) {
                for (TypeDefine td2 : l2) {
                    if (!td1.equals(td2)) {
                        ret.add(field);
                        break;
                    }
                }
            }
        }

        return ret;
    }

    public MappingConfig deepCopy() throws Exception {
        JSONObject jsonObject = this.toJson();
        return new MappingConfig(jsonObject);
    }

    private static final String DEFAULT_TYPE_STR = "_default_";
    public void mergeDefault(MappingConfig mappings) {
        TypeConfig typeMapping = mappings.getMapping().get(DEFAULT_TYPE_STR);
        if(typeMapping!=null) {
            this.mapping.put(DEFAULT_TYPE_STR, typeMapping);
        }
    }

    // m的配置覆盖当前mapping。粒度是field层面
    public void merge(MappingConfig m) {
        for(String type : m.mapping.keySet()) {
            if(type.equals(DEFAULT_TYPE_STR)) {
                continue;
            }

            TypeConfig tm = m.mapping.get(type);

            if(this.mapping.containsKey(type)) {
                this.mapping.get(type).merge(tm);
            } else {
                this.mapping.put(type, tm);
            }
        }
    }

    public boolean haveDefault() {
        return mapping.containsKey(DEFAULT_TYPE_STR);
    }

    public void removeDefault() {
        mapping.remove(DEFAULT_TYPE_STR);
    }

    public boolean isJustDefault() {
        if(mapping.size()==0) {
            return true;
        }

        if(mapping.size()==1 && mapping.containsKey(DEFAULT_TYPE_STR)) {
            return true;
        }

        return false;
    }
}
