package io.github.hefrankeleyn.hefmq.client;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class HefBroker {

    private final Map<String, HefMessageQueue> queueMap = Maps.newConcurrentMap();


    public HefMessageQueue findMessageQueue(String topic) {
        return queueMap.get(topic);
    }

    public HefMessageQueue createTopic(String topic) {
        queueMap.putIfAbsent(topic, new HefMessageQueue(topic));
        return queueMap.get(topic);
    }

    public HefProducer createProducer() {
        return new HefProducer(this);
    }

    public HefConsumer createConsumer(String topic) {
        HefConsumer consumer = new HefConsumer(this);
        consumer.subscribe(topic);
        return consumer;
    }
}
