package com.didichuxing.datachannel.arius.fastindex.server.common.es.mapping;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 * TypeDefine操作类
 *
 * 主要是完成高低版本兼容
 *
 * Created by d06679 on 2019/3/6.
 */
public class TypeDefineOperator {

    private static final String TYPE_STR                       = "type";

    private static final String NESTED_STR                     = "nested";

    private static final String OBJECT_STR                     = "object";

    private static final String FIELDS_STR                     = "fields";

    private static final String RAW_STR                        = "raw";

    private static final String INDEX_STR                      = "index";

    private static final String DOC_VALUES_STR                 = "doc_values";

    private static final String IGNORE_ABOVE_STR               = "ignore_above";

    private static final String INCLUDE_IN_ALL_STR             = "include_in_all";

    private static final String PRECISION_STEP_STR             = "precision_step";

    private static final String ES_HIGH_TYPE_TEXT_STR          = "text";

    private static final String ES_HIGH_TYPE_KEYWORD_STR       = "keyword";

    private static final String ES_LOW_TYPE_STRING_STR         = "string";

    private static final String ES_LOW_STRING_NOT_ANALYZED_STR = "not_analyzed";

    private static final String ES_LOW_STRING_FIELDDATA_STR    = "fielddata";

    /**
     * 是否需要忽略mapping优化
     * @return
     */
    public static boolean isNotOptimze(JSONObject define) {
        if (define.containsKey(TYPE_STR)) {
            String v = define.getString(TYPE_STR);
            if (NESTED_STR.equalsIgnoreCase(v) || OBJECT_STR.equalsIgnoreCase(v)) {
                return true;
            }
        } else if (define.containsKey(FIELDS_STR)) {
            //                    \"artifact\": {\n"
            //                              \"type\": \"string\",\n"
            //                              \"fields\": {\n"
            //                                \"raw\": {\n"
            //                                  \"ignore_above\": 1024,\n"
            //                                  \"index\": \"not_analyzed\",\n"
            //                                   \"type\": \"string\"\n"
            //                                }\n"
            //                              }\n"
            if (define.get(FIELDS_STR) instanceof JSONObject) {
                JSONObject fieldsObj = define.getJSONObject(FIELDS_STR);
                if (fieldsObj.containsKey(RAW_STR)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 获取类型
     *
     * @return
     */
    public static String getType(JSONObject define) {
        if (define != null && define.containsKey(TYPE_STR)) {
            return define.getString(TYPE_STR);
        }

        return null;
    }

    public static boolean isHighVersionString(JSONObject define) {
        String type = getType(define);
        return !StringUtils.isBlank(type)
               && (type.equals(ES_HIGH_TYPE_TEXT_STR) || type.equals(ES_HIGH_TYPE_KEYWORD_STR));
    }

    public static boolean isLowVersionString(JSONObject define) {
        String type = getType(define);
        return !StringUtils.isBlank(type) && type.equals(ES_LOW_TYPE_STRING_STR);
    }

    public static boolean isIndexOff(JSONObject define) {

        if (isHighVersionString(define)) {
            //高版本的es
            if ("false".equalsIgnoreCase(define.getString(INDEX_STR))
                && "true".equalsIgnoreCase(define.getString(DOC_VALUES_STR))) {
                return true;
            }
        } else {
            if ("no".equalsIgnoreCase(define.getString(INDEX_STR))
                && "true".equalsIgnoreCase(define.getString(DOC_VALUES_STR))) {
                return true;
            }
        }

        return false;
    }

    /**
     * 设置成不检索
     *
     */
    public static void setIndexOff(JSONObject define) {
        if (isHighVersionString(define)) {
            define.put(INDEX_STR, "false");
        } else {
            define.put(INDEX_STR, "no");
        }
    }

    /**
     * 设置成检索
     *
     */
    public static void setIndexOn(JSONObject define) {
        if (isHighVersionString(define)) {
            define.remove(INDEX_STR);
            define.put(TYPE_STR, ES_HIGH_TYPE_KEYWORD_STR);
        } else {
            define.put(TYPE_STR, ES_LOW_STRING_NOT_ANALYZED_STR);
        }
    }

    public static boolean isDocValuesOff(JSONObject define) {
        if (isHighVersionString(define)) {
            if ("false".equalsIgnoreCase(define.getString(INDEX_STR))
                && !"true".equalsIgnoreCase(define.getString(DOC_VALUES_STR))) {
                return true;
            }
        } else {
            if ("no".equalsIgnoreCase(define.getString(INDEX_STR))
                && !"true".equalsIgnoreCase(define.getString(DOC_VALUES_STR))) {
                return true;
            }
        }

        return false;
    }

    /**
     * 设置成不支持排序
     */
    public static void setDocValuesOff(JSONObject define) {
        define.put(DOC_VALUES_STR, false);
    }

    /**
     * 设置成支持排序
     */
    public static void setDocValuesOn(JSONObject define) {
        define.put(DOC_VALUES_STR, true);
    }

    private static final String ENABLED = "enabled";
    private static final String DYNAMIC = "dynamic";
    private static final String DOC_VALUE = "doc_values";
    private static final String INDEX = "index";
    public static boolean isEquals(JSONObject define, Object obj) {
        if (!(obj instanceof TypeDefine)) {
            return false;
        }

        TypeDefine t = (TypeDefine) obj;

        JSONObject j1 = (JSONObject) define.clone();
        JSONObject j2 = (JSONObject) t.getDefine().clone();

        j1.remove(IGNORE_ABOVE_STR);
        j2.remove(IGNORE_ABOVE_STR);

        // 去除默认值配置
        removeDefaultValue(j1, ENABLED, "true");
        removeDefaultValue(j1, DYNAMIC, "false");
        removeDefaultValue(j1, DOC_VALUE, "true");
        removeDefaultValue(j1, INDEX, "true");

        removeDefaultValue(j2, ENABLED, "true");
        removeDefaultValue(j2, DYNAMIC, "false");
        removeDefaultValue(j2, DOC_VALUE, "true");
        removeDefaultValue(j2, INDEX, "true");

        return j1.equals(j2);
    }

    private static void removeDefaultValue(JSONObject def, String typeName, String defaultValue) {
        if(!def.containsKey(typeName)) {
            return;
        }

        String value = def.getString(typeName);
        if(defaultValue.equals(value)) {
            def.remove(typeName);
        }
    }

    public static JSONObject toJson(JSONObject define, ESVersion version) {

        if (version == ESVersion.ES651) {
            dealHighLevelType(define);

            if (define.containsKey(FIELDS_STR)) {
                JSONObject fields = define.getJSONObject(FIELDS_STR);
                for (String key : fields.keySet()) {
                    JSONObject fieldType = fields.getJSONObject(key);

                    dealHighLevelType(fieldType);
                }
            }
        }

        return define;
    }

    private static void dealHighLevelType(JSONObject define) {
        if (isLowVersionString(define)) {
            // 处理String
            if (define.containsKey(INDEX_STR)) {
                if (ES_LOW_STRING_NOT_ANALYZED_STR.equals(define.get(INDEX_STR))) {
                    // keyword
                    define.remove(INDEX_STR);
                    define.put(TYPE_STR, ES_HIGH_TYPE_KEYWORD_STR);
                } else if ("no".equals(define.get(INDEX_STR))) {
                    // keyword and not index
                    define.put(INDEX_STR, false);
                    define.remove(IGNORE_ABOVE_STR);
                    define.put(DOC_VALUES_STR, false);
                    define.put(TYPE_STR, ES_HIGH_TYPE_KEYWORD_STR);
                } else {
                    define.remove(INDEX_STR);
                    define.remove(IGNORE_ABOVE_STR);
                    define.put(TYPE_STR, ES_HIGH_TYPE_TEXT_STR);
                }
            } else {
                define.remove(IGNORE_ABOVE_STR);
                define.put(TYPE_STR, ES_HIGH_TYPE_TEXT_STR);
            }
        } else {
            // 处理其他字段
            if (define.containsKey(INDEX_STR) && "no".equals(define.get(INDEX_STR))) {
                define.put(INDEX_STR, false);
                define.put(DOC_VALUES_STR, false);
            }
        }

        // 移除高版本不兼容字段
        define.remove(ES_LOW_STRING_FIELDDATA_STR);
        define.remove(INCLUDE_IN_ALL_STR);
        define.remove(PRECISION_STEP_STR);
    }
}
