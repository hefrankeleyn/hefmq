package io.github.hefrankeleyn.hefmq.model;

/**
 * @Date 2024/7/22
 * @Author lifei
 */
public class Result<T> {
    private T data;
    private Integer code;

    public Result() {
    }

    public Result(T data, Integer code) {
        this.data = data;
        this.code = code;
    }

    public static <M> Result<M> ok(M data) {
        return new Result<>(data, CodeEnum.SUCCESS.code);
    }

    public boolean isSuccess() {
        return code == CodeEnum.SUCCESS.code;
    }

    public enum CodeEnum {
        SUCCESS(1), FAIL(0);
        final int code;

        CodeEnum(int code) {
            this.code = code;
        }
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }
}
