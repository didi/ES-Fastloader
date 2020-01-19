package com.didichuxing.fastindex.controller;

public enum ResultType {

    SUCCESS(0, "操作成功"),

    FAIL(1, "内部错误，请联系管理员");

    private int code;
    private String message;

    ResultType(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

}
