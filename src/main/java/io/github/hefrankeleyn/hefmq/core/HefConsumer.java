package io.github.hefrankeleyn.hefmq.core;

import static com.google.common.base.Preconditions.*;

import java.util.Objects;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class HefConsumer {

    private final HefBroker hefBroker;
    private HefMessageQueue messageQueue;

    public HefConsumer(HefBroker hefBroker) {
        this.hefBroker = hefBroker;
    }

    public void subscribe(String topic) {
        this.messageQueue = hefBroker.findMessageQueue(topic);
        checkState(Objects.nonNull(messageQueue), "topic [%s] not found", topic);
    }

    public HefMessage<?> poll(long timeout) {
        checkState(Objects.nonNull(messageQueue), "Please subscribe first");
        return messageQueue.poll(timeout);
    }

    public void addMessageListener(HefMessageListener hefMessageListener) {
        this.messageQueue.listener(hefMessageListener);
    }
}
