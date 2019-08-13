package com.didi.bigdata.mr2es.alarm;

/**
 *
 */
public enum AlarmIfCall {
    /**
     * 启用电话报警
     */
    CALL_NO(0, "打电话"),
    /**
     * 不启用电话报警
     */
    CALL(1, "不打电话");

    private int level;
    private String remark;

    private AlarmIfCall(int level, String remark) {
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
