package com.didichuxing.fastindex.controller;


public class Result<T> {
    private int code;
    private String message;
    private T data;

    public Result(T data) {
        this.data = data;
        this.code = ResultType.SUCCESS.getCode();
        this.message = ResultType.SUCCESS.getMessage();
    }

    public Result(ResultType resultType) {
        this.code = resultType.getCode();
        this.message = resultType.getMessage();
    }

    public Result(ResultType resultType, String message) {
        this.code = resultType.getCode();
        this.message = message;
    }

    public Result(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Result{" +
                "code=" + code +
                ", message='" + message + '\'' +
                ", data=" + data +
                '}';
    }


}
