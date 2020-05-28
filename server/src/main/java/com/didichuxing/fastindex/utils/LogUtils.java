package com.didichuxing.fastindex.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class LogUtils {
    private static final SimpleDateFormat dateFormat         = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String LOG_FORMAT = "%s - %s - %s";

    private static final Set<String> IGNORE_STR = new HashSet<>();
    public static synchronized void addIgnoreStr(String str) {
        IGNORE_STR.add(str);
    }

    private static String ignoreStr(String str) {
        for(String ignoreStr : IGNORE_STR) {
            if(ignoreStr==null || ignoreStr.length()==0) {
                continue;
            }

            str = str.replaceAll(ignoreStr, "******");
        }

        return str;
    }

    public static void info(String info) {
        long time = System.currentTimeMillis();
        String timeStr = dateFormat.format(time);

        System.out.println(String.format(LOG_FORMAT, timeStr, "INFO", ignoreStr(info)));
    }

    public static void error(String info) {
        long time = System.currentTimeMillis();
        String timeStr = dateFormat.format(time);
        System.out.println(String.format(LOG_FORMAT, timeStr, "ERROR", ignoreStr(info)));
        log.error(info);
    }

    public static void error(String info, Throwable t) {
        long time = System.currentTimeMillis();
        String timeStr = dateFormat.format(time);
        System.out.println(String.format(LOG_FORMAT, timeStr, "ERROR", ignoreStr(info)));

        if(t!=null) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                t.printStackTrace(pw);
                String sStackTrace = sw.toString(); // stack trace as a string
                System.out.println(sStackTrace);
            } catch (Throwable tt) {
                System.out.println("convert exception error, msg:" + tt.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        info("hello");
        error("hello");


    }
}

