package com.didichuxing.datachannel.arius.fastindex.mr.remote;

/* 环境信息 */
public enum EnvEnum {
    ONLINE("online"),PRE("pre"), TEST("test");

    EnvEnum(String code) {
        this.code = code;
    }

    private String code;

    public String getCode() {
        return code;
    }

    public static EnvEnum valueFrom(String code) throws Exception {
        if (code == null) {
            throw new Exception("unknown code, code:" + code);
        }

        for (EnvEnum value : EnvEnum.values()) {
            if (value.getCode().equalsIgnoreCase(code)) {
                return value;
            }
        }

        throw new Exception("unknown code, code:" + code);
    }
}
