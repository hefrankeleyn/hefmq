package io.github.hefrankeleyn.hefmq.model;

import com.google.common.base.MoreObjects;

/**
 * @Date 2024/7/22
 * @Author lifei
 */
public class MessageSubscription {
    private String topic;
    private String consumerId;
    private Integer offset = -1;

    public MessageSubscription() {
    }

    public MessageSubscription(String topic, String consumerId, Integer offset) {
        this.topic = topic;
        this.consumerId = consumerId;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(MessageSubscription.class)
                .add("topic", topic)
                .add("consumerId", consumerId)
                .add("offset", offset)
                .toString();
    }
}
