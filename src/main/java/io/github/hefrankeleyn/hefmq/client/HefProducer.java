package io.github.hefrankeleyn.hefmq.client;

import io.github.hefrankeleyn.hefmq.model.HefMessage;

import static com.google.common.base.Preconditions.*;

import java.util.Objects;

/**
 * 消息生产者
 * @Date 2024/7/20
 * @Author lifei
 */
public class HefProducer {

    private final HefBroker hefBroker;

    public HefProducer(HefBroker hefBroker) {
        this.hefBroker = hefBroker;
    }

    public boolean send(String topic, HefMessage<?> hefMessage) {
        HefMessageQueue hefMessageQueue = hefBroker.findMessageQueue(topic);
        checkState(Objects.nonNull(hefMessageQueue), "topic [%s] not found", topic);
        return hefMessageQueue.send(hefMessage);
    }
}
