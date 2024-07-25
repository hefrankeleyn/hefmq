package io.github.hefrankeleyn.hefmq.client;

import com.google.common.base.MoreObjects;
import io.github.hefrankeleyn.hefmq.model.HefMessage;

import static com.google.common.base.Preconditions.*;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class HefConsumer {

    private static AtomicLong idGenerator = new AtomicLong(0);
    private String id;
    private final HefBroker hefBroker;
    private HefMessageQueue messageQueue;

    public HefConsumer(HefBroker hefBroker) {
        this.hefBroker = hefBroker;
        this.id = "consumer-" + idGenerator.getAndIncrement();
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(HefConsumer.class)
                .add("id", id)
                .toString();
    }
}
