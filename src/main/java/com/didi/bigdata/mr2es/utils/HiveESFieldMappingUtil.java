package com.didi.bigdata.mr2es.utils;

import java.util.HashMap;

/**
 * @author tony-xutian
 * @version 1.0
 * Created on 2016/4/5.
 */
public class HiveESFieldMappingUtil {
    private static final HiveESFieldMappingUtil _INSTANCE=
            new HiveESFieldMappingUtil();
    private HashMap<String,String> mapper;

    private HiveESFieldMappingUtil(){
        this.mapper= new HashMap<>();
        this.mapper.put("string","string");
        this.mapper.put("int","integer");
        this.mapper.put("bigint","long");
        this.mapper.put("decimal","double");
    }

    public static HiveESFieldMappingUtil getInstance(){
        return _INSTANCE;
    }

    public String getESFieldsMapping(String key){
        String retValue;

        retValue=this.mapper.get(key.trim().toLowerCase());
        if(retValue==null){
            retValue="string";
        }

        return retValue;
    }
}
