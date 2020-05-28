package com.didichuxing.datachannel.arius.fastindex.server.utils;

import lombok.extern.slf4j.Slf4j;

/**
 * 索引大小转换工具类
 */
@Slf4j
public class SizeUtil {
    /**
     * 得到字节数
     *
     * @param proStoreSize
     * @return
     */
    public static Long getUnitSize(String proStoreSize) {
        final long C0 = 1L;
        final long C1 = C0 * 1024L;
        final long C2 = C1 * 1024L;
        final long C3 = C2 * 1024L;
        final long C4 = C3 * 1024L;
        final long C5 = C4 * 1024L;
        long bytes = 0L;

        try {
            String lowerSValue = proStoreSize.trim();
            if (lowerSValue.endsWith("k")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * C1);
            } else if (lowerSValue.endsWith("kb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * C1);
            } else if (lowerSValue.endsWith("m")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * C2);
            } else if (lowerSValue.endsWith("mb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * C2);
            } else if (lowerSValue.endsWith("g")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * C3);
            } else if (lowerSValue.endsWith("gb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * C3);
            } else if (lowerSValue.endsWith("t")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * C4);
            } else if (lowerSValue.endsWith("tb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * C4);
            } else if (lowerSValue.endsWith("p")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * C5);
            } else if (lowerSValue.endsWith("pb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * C5);
            } else if (lowerSValue.endsWith("b")) {
                bytes = Long.parseLong(lowerSValue.substring(0, lowerSValue.length() - 1).trim());
            } else if (lowerSValue.equals("-1")) {
                // Allow this special value to be unit-less:
                bytes = -1;
            } else if (lowerSValue.equals("0")) {
                // Allow this special value to be unit-less:
                bytes = 0;
            } else {
                log.error("getUnitSize error {}", proStoreSize);
            }
        } catch (Exception e) {
            log.error("getUnitSize error {}", proStoreSize, e);
        }
        return bytes;
    }

    /**
     * 得到单位化的大小
     *
     * @param bytes
     * @return
     */
    public static String getUnitSize(long bytes) {
        final long C0 = 1L;
        final long C1 = C0 * 1024L;
        final long C2 = C1 * 1024L;
        final long C3 = C2 * 1024L;
        final long C4 = C3 * 1024L;
        final long C5 = C4 * 1024L;
        double value = bytes;
        String suffix = "b";
        if (bytes >= C5) {
            value = ((double) bytes) / C5;
            suffix = "pb";
        } else if (bytes >= C4) {
            value = ((double) bytes) / C4;
            suffix = "tb";
        } else if (bytes >= C3) {
            value = ((double) bytes) / C3;
            suffix = "gb";
        } else if (bytes >= C2) {
            value = ((double) bytes) / C2;
            suffix = "mb";
        } else if (bytes >= C1) {
            value = ((double) bytes) / C1;
            suffix = "kb";
        }

        return String.format("%f%s", value, suffix);
    }

}
