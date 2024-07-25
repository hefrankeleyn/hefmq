package io.github.hefrankeleyn.hefmq.client;

import com.google.common.base.MoreObjects;
import static com.google.common.base.Preconditions.*;
import io.github.hefrankeleyn.hefmq.model.HefMessage;

import java.util.List;
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
    private String topic;

    private HefMessageListener hefMessageListener;

    public HefConsumer(HefBroker hefBroker) {
        this.hefBroker = hefBroker;
        this.id = "consumer-" + idGenerator.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        hefBroker.subscribe(topic, id);
    }

    public void unsubscribe() {
        checkState(Objects.nonNull(topic) && !topic.isBlank(), "Please subscribe topic first: %s", topic);
        hefBroker.unsubscribe(topic, id);
    }



    public HefMessage<?> receive() {
        checkState(Objects.nonNull(topic) && !topic.isBlank(), "Please subscribe topic first: %s", topic);
        return hefBroker.receive(topic, id);
    }

    public List<HefMessage<?>> batchReceive(int size) {
        checkState(Objects.nonNull(topic) && !topic.isBlank(), "Please subscribe topic first: %s", topic);
        return hefBroker.batchReceive(topic, id, size);
    }

    public void ack(int offset) {
        checkState(Objects.nonNull(topic) && !topic.isBlank(), "Please subscribe topic first: %s", topic);
        hefBroker.ack(topic, id, offset);
    }

    public void ackMessage(HefMessage<?> hefMessage) {
        if (Objects.isNull(hefMessage)) {
            return;
        }
        int offset = Integer.parseInt(hefMessage.getHeaders().get(HefMessage.OFFSET_KEY));
        ack(offset);
    }

    public void addMessageListener(HefMessageListener hefMessageListener) {
        this.hefMessageListener = hefMessageListener;
        hefBroker.addConsumer(this);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public HefMessageListener getHefMessageListener() {
        return hefMessageListener;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(HefConsumer.class)
                .add("id", id)
                .add("topic", topic)
                .toString();
    }


}
