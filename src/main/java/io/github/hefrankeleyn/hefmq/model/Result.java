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

    public enum CodeEnum {
        SUCCESS(1), FAIL(0);
        final int code;

        CodeEnum(int code) {
            this.code = code;
        }
    }
}
