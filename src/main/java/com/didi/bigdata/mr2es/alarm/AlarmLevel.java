package com.didi.bigdata.mr2es.alarm;

/**
 *
 */
public enum AlarmLevel {
    /**
     * 解除报警，未使用
     */
    RELIEVE(0, "解除报警"),

    /**
     * 一级报警
     */
    LEVEL1(1, "一级报警"),

    /**
     * 二级报警
     */
    LEVEL2(2, "二级报警"),

    /**
     * 三级报警
     */
    LEVEL3(3, "三级报警"),

    /**
     * 四级报警
     */
    LEVEL4(4, "四级报警");

    private int level;
    private String remark;

    private AlarmLevel(int level, String remark) {
        this.level = level;
        this.remark = remark;
    }

    public int getLevel() {
        return level;
    }

    public String getRemark() {
        return remark;
    }
}
