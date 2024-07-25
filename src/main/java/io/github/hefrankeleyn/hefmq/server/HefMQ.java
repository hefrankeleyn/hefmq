package io.github.hefrankeleyn.hefmq.server;

import com.google.common.base.MoreObjects;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Maps;
import io.github.hefrankeleyn.hefmq.model.HefMessage;
import io.github.hefrankeleyn.hefmq.model.MessageSubscription;

import java.util.Map;
import java.util.Objects;

/**
 * @Date 2024/7/22
 * @Author lifei
 */
public class HefMQ {

    private static final Map<String, HefMQ> topicMQMap = Maps.newHashMap();
    private static final Map<String, MessageSubscription> subscriptionMap = Maps.newHashMap();

    private static final String DEFAULT_TOPIC = "com.hef.demo";
    static {
        topicMQMap.put(DEFAULT_TOPIC, new HefMQ(DEFAULT_TOPIC));
    }

    private String topic;
    private HefMessage<?>[] queue = new HefMessage[1024*10];
    private int index;

    public HefMQ(){}

    public HefMQ(String topic) {
        this.topic = topic;
    }

    public void subscribe(String topic, String consumerId) {
        subscriptionMap.putIfAbsent(consumerId, new MessageSubscription(topic, consumerId, -1));
    }

    public void unsubscribe(String consumerId) {
        subscriptionMap.remove(consumerId);
    }

    public static void subscribeTopic(String topic, String consumerId) {
        HefMQ hefMQ = topicMQMap.get(topic);
        if (Objects.nonNull(hefMQ)) {
            hefMQ.subscribe(topic, consumerId);
        }
    }

    public static void unsubscribeTopic(String topic, String consumerId) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        hefMQ.unsubscribe(consumerId);
    }

    public static Integer sendMessage(String topic, HefMessage<?> message) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        return hefMQ.send(message);
    }

    public static HefMessage<?> receiveMessage(String topic, String consumerId, Integer i) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        checkState(Objects.nonNull(subscriptionMap.get(consumerId)), "Please subscribe topic first: %s", topic);
        return hefMQ.receive(i);
    }

    public static Integer ackMessage(String topic, String consumerId, Integer offset) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        checkState(Objects.nonNull(subscriptionMap.get(consumerId)), "Please subscribe topic first: %s", topic);
        MessageSubscription messageSubscription = subscriptionMap.get(consumerId);
        if (Objects.nonNull(offset) && offset>messageSubscription.getOffset() && offset<=hefMQ.getIndex()) {
            messageSubscription.setOffset(offset);
            return offset;
        }else {
            return -1;
        }
    }

    /**
     * 使用此方法，需要手动调用ack，更新offset
     * @param topic
     * @param consumerId
     * @return
     */
    public static HefMessage<?> receiveMessage(String topic, String consumerId) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        checkState(Objects.nonNull(subscriptionMap.get(consumerId)), "Please subscribe topic first: %s", topic);
        MessageSubscription messageSubscription = subscriptionMap.get(consumerId);
        return hefMQ.receive(messageSubscription.getOffset());
    }

    /**
     * 发送消息
     * @param hefMessage
     * @return
     */
    public int send(HefMessage<?> hefMessage) {
        if (index>=queue.length) {
            return -1;
        }
        queue[index++] = hefMessage;
        return index;
    }

    /**
     * 获取消息
     * @return
     */
    public HefMessage<?> receive(int i) {
        if (index>0 && i>=0 && i<=index) {
            return queue[i];
        }
        return null;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public HefMessage<?>[] getQueue() {
        return queue;
    }

    public void setQueue(HefMessage<?>[] queue) {
        this.queue = queue;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(HefMQ.class)
                .add("topic", topic)
                .add("queue", queue)
                .add("index", index)
                .toString();
    }
}
