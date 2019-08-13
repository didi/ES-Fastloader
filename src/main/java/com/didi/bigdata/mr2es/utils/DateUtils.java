package com.didi.bigdata.mr2es.utils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间工具
 * @author WangZhuang
 * @date 2018年9月28日
 *
 */
public class DateUtils {
    
    private static final SimpleDateFormat simpleDateFormat =
            new SimpleDateFormat("yyyyMMdd");

    
    /**
     * 通过String类型日期获取long时间戳
     * @param dateString
     * @return
     */
    public static synchronized long getTimestamp(String dateString) {
        Date date = parseDate(dateString);
        if (date == null) {
            return System.currentTimeMillis();
        }
        return date.getTime();
    }
    
    /**
     * 将String型日期转成Date型日期
     * @param dateString
     * @return
     */
    public static synchronized Date parseDate(String dateString){
        if (dateString == null) {
            return null;
        }
    	try {
			return simpleDateFormat.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	return null;
    }

    public static void main(String[] args) {

    }
}
