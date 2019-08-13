package com.didi.bigdata.mr2es.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * to linejson
 */
public class ESJson {
    private static final Logger logger = LoggerFactory.getLogger(ESJson.class);
    private static SimpleDateFormat simpleDateFormat =
            new SimpleDateFormat("yyyy-MM-dd");
    private static String secondsTimeStampRegex = "\\d{10}";//匹配精度是秒的时间戳

    private DateTimeFormatter timeFormatter;
    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter statFormatter;
    private String timeRegex;
    private String dateRegex;
    private String statRegex;
    private long timestamp;
    private int timezoneOffset;
    private static final Set<String> NAValues = new HashSet<String>() {
        {
            add("N/A");
            add("null");
        }
    };

    public ESJson(DateTimeFormatter dateFormatter, String dateRegex,
                  DateTimeFormatter statFormatter, String statRegex,
                  DateTimeFormatter timeFormatter, String timeRegex,
                  long timestamp, int timezoneOffset) {
        this.dateFormatter = dateFormatter;
        this.dateRegex = dateRegex;
        this.statFormatter = statFormatter;
        this.statRegex = statRegex;
        this.timeFormatter = timeFormatter;
        this.timeRegex = timeRegex;
        this.timestamp = timestamp;
        this.timezoneOffset = timezoneOffset;
    }

    public JSONObject buildJson(List<Object> valueList,
                                List<String> fieldNames,
                                Map<String, String[]> defaultValues) {
        Object value;
        JSONObject jsonRecord = new JSONObject();
        String[] defaultData = null;
        String mysqlType = null;
        String defaultValueStr = null;
        String fieldName = null;
        String dafaultDate = "1970-01-01";
        Set<String> defaultField = defaultValues.keySet();

        int size = fieldNames.size();
        for (int i = 0; i < size; i++) {
            fieldName = fieldNames.get(i);
//            String fieldType = this.fieldTypes.get(i);

            if (defaultField.contains(fieldName)) {
                defaultData = defaultValues.get(fieldName);
                mysqlType = defaultData[0];
                defaultValueStr = defaultData[1];
//                try {
                value = valueList.get(i);
//                } catch (HCatException e) {
//                    value = null;
//                    logger.error("System get value touch off Exception", e);
//                }

                if (value != null && !value.toString().trim().equals("")) {
                    if (mysqlType.equalsIgnoreCase("bigint")) {
                        jsonRecord.put(fieldName, getLong(value.toString()));
                    } else if (mysqlType.equalsIgnoreCase("int")) {
                        jsonRecord.put(fieldName, getInteger(value.toString()));
                    } else if (mysqlType.startsWith("decimal")) {
                        jsonRecord.put(fieldName, getDouble(value.toString()));
                    } else if (mysqlType.equalsIgnoreCase("string")) {
                        if (fieldName.equals("itags")) {
                            String[] tagNames = value.toString().split(",");
                            List<String> tags = Arrays.asList(tagNames);
                            jsonRecord.put(fieldName, tags);
                        } else if (fieldName.equals("method")) {
                            String[] values = value.toString().split(",");
                            jsonRecord.put(fieldName, values);
                        } else {
                            jsonRecord.put(fieldName, value.toString());
                        }
                    } else if (mysqlType.contains("date")) {
                        String val = value.toString().trim();
                        if (val.equals("9999-99-99 99:99:99") ||
                                val.equals("0000-00-00 00:00:00") ||
                                val.equals("9999-99-99") ||
                                val.equals("0000-00-00")) {
                            jsonRecord.put(fieldName, dafaultDate);
                        } else {
                            jsonRecord.put(fieldName, getDateMsg(value.toString()));
                        }
                    } else if (mysqlType.equals("geo")) {
                        ArrayList<Double> lnglatDouble = new ArrayList<>();
                        String[] lnglatString = value.toString().split("#");
                        if (lnglatString.length == 2) {
                            double lngDouble = Double.parseDouble(lnglatString[0]);
                            double latDouble = Double.parseDouble(lnglatString[1]);
                            if (lngDouble > -180.0 && lngDouble < 180.0 &&
                                    latDouble > -90.0 && latDouble < 90.0) {
                                lnglatDouble.add(lngDouble);
                                lnglatDouble.add(latDouble);
                                JSONObject geo_shape = new JSONObject();
                                geo_shape.put("type", "point");
                                geo_shape.put("coordinates", lnglatDouble);
                                jsonRecord.put(fieldName, geo_shape);
                            }
                        }
                    } else if (mysqlType.equals("range")) {
                        jsonRecord.put(fieldName, getDouble(value.toString()));
                    }
                    // current for type : city / district / radio
                    else {
                        jsonRecord.put(fieldName, value.toString());
                    }
                } else {
                    if (mysqlType.equalsIgnoreCase("bigint")) {
                        jsonRecord.put(fieldName, getLong(defaultValueStr));
                    } else if (mysqlType.equalsIgnoreCase("int")) {
                        jsonRecord.put(fieldName, getInteger(defaultValueStr));
                    } else if (mysqlType.startsWith("decimal")) {
                        jsonRecord.put(fieldName, getDouble(defaultValueStr));
                    } else if (mysqlType.equalsIgnoreCase("string")) {
                        jsonRecord.put(fieldName, defaultValueStr);
                    } else if (mysqlType.equalsIgnoreCase("date")) {
                        jsonRecord.put(fieldName, dafaultDate);
                    } else if (mysqlType.equalsIgnoreCase("geo")) {
                        //geo类型如果取不到数据不作处理
                    } else {
                        jsonRecord.put(fieldName, defaultValueStr);
                    }
                }
                jsonRecord.put("timestamp", timestamp);
            }
        }
        return jsonRecord;
    }

    private String getDateMsg(String value){
        try {
            String result = null;
            Matcher timeRegexMatcher = Pattern.compile(timeRegex).matcher(value);
            Matcher dateRegexMatcher = Pattern.compile(dateRegex).matcher(value);
            Matcher statRegexMatcher = Pattern.compile(statRegex).matcher(value);
            Matcher secondsTimeStampMatcher = Pattern.compile(secondsTimeStampRegex).matcher(value);
            if (timeRegexMatcher.matches()){
                result = timeFormatter.parseDateTime(value)
                        .plusHours(timezoneOffset).toString("yyyy-MM-dd");
            }
            else if (dateRegexMatcher.matches()){
                result = dateFormatter.parseDateTime(value)
                        .plusHours(timezoneOffset).toString("yyyy-MM-dd");
            }
            else if(statRegexMatcher.matches()){
                result = statFormatter.parseDateTime(value)
                        .plusHours(timezoneOffset).toString("yyyy-MM-dd");
            }
            else if(secondsTimeStampMatcher.matches()){
                Date date = new Date(Long.valueOf(value) * 1000);
                result = simpleDateFormat.format(date);
            }
            else {
                Date date = new Date(Long.valueOf(value));
                result = simpleDateFormat.format(date);
            }

            return result;
        }
        catch(NumberFormatException e){
            //logger.error("Date Parse Exception: " + value, e);
            return "1970-01-01";
        }
    }

    private int getInteger(String val){
        try{
            return Integer.parseInt(val);
        }
        catch (NumberFormatException e){
            logger.error("Integer Number Format Exception: " + val, e);
            return 0;
        }
    }

    private double getDouble(String val){
        try{
            return Double.parseDouble(val);
        }
        catch (NumberFormatException e){
            if (!matchNAValues(val)) {
                logger.error("Double Number Format Exception: " + val, e);
            }
            return 0.0D;
        }
    }

    private long getLong(String val){
        try{
            return Long.parseLong(val);
        }
        catch (NumberFormatException e){
            logger.error("Long Number Format Exception: " + val, e);
            return 0L;
        }
    }

    private boolean matchNAValues(String val) {
        return NAValues.contains(val);
    }
}
