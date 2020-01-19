package com.didichuxing.datachannel.arius.fastindex.transform.normal.type;

import com.alibaba.fastjson.JSONArray;

import java.util.HashSet;
import java.util.Set;

public class StringType extends Type {
    public static final Set<String> matchESTypes = new HashSet<>();
    public static final Set<String> matchHiveTypes = new HashSet<>();
    static {
        matchESTypes.add("string".toUpperCase());
        matchESTypes.add("text".toUpperCase());
        matchESTypes.add("keyword".toUpperCase());
        matchESTypes.add("date".toUpperCase());

        matchHiveTypes.add("string".toUpperCase());
        matchHiveTypes.add("char".toUpperCase());
        matchHiveTypes.add("varchar".toUpperCase());
        matchHiveTypes.add("binary".toUpperCase());
        matchHiveTypes.add("date".toUpperCase());
        matchHiveTypes.add("timestamp".toUpperCase());
    }

    private boolean removeBracket; // 是否去除两边的中括号
    private boolean null2Null;
    private Set<String> strToArray = new HashSet<>();


    public StringType(String name, boolean removeBracket, boolean null2Null, Set<String> strToArray) {
        super(name);

        this.removeBracket = removeBracket;
        this.null2Null = null2Null;
        if(strToArray!=null) {
            this.strToArray.addAll(strToArray);
        }
    }

    @Override
    public Object tranform(Object value) {
        if(value==null) {
            if(null2Null) {
                return null;
            } else {
                return "";
            }
        }

        String str = value.toString();

        if(strToArray.contains(name)) {
            JSONArray jsonArray = toArray(str);
            if(jsonArray!=null) {
                return jsonArray;
            }
        }

        if(removeBracket) {
            int tag = 0;
            if(str.startsWith("[")) {
                str = str.substring(1);
                tag++;
            }

            if(str.endsWith("]")) {
                str = str.substring(0, str.length()-1);
                tag++;
            }

            if(tag==2) {
                str = str.replaceAll(" ", "");
            }
        }

        return str;
    }
}
