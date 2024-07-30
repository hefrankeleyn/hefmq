package io.github.hefrankeleyn.hefmq.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class HefMessage<M> {

    public static final String OFFSET_KEY = "X-offset";

    private static final AtomicLong idGenerator = new AtomicLong(1);
    // 只能复制不能修改，根据id 识别出是不同的消息
    private Long id;
    // 消息体
    private M body;
    // 系统参数：比如 消息优先级
    private Map<String, String> headers = Maps.newHashMap();
    // 业务参数：比如 消息来源
    private Map<String, String> properties = Maps.newHashMap();

    public HefMessage(){}

    public HefMessage(Long id, M body) {
        this.id = id;
        this.body = body;
    }

    public HefMessage(M body) {
        this.id = idGenerator.getAndIncrement();
        this.body = body;
    }

    public static <T> HefMessage<T> createHefMessage(T body) {
        return new HefMessage<>(body);
    }



    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public M getBody() {
        return body;
    }

    public void setBody(M body) {
        this.body = body;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(HefMessage.class)
                .add("id", id)
                .add("body", body)
                .add("headers", headers)
                .add("properties", properties)
                .toString();
    }
}
