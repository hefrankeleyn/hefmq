package io.github.hefrankeleyn.hefmq.server;

import com.google.common.base.MoreObjects;
import static com.google.common.base.Preconditions.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.hefrankeleyn.hefmq.model.HefMessage;
import io.github.hefrankeleyn.hefmq.model.MessageSubscription;
import io.github.hefrankeleyn.hefmq.store.HefStore;
import io.github.hefrankeleyn.hefmq.store.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
    private static final Logger log = LoggerFactory.getLogger(HefMQ.class);

    static {
        topicMQMap.put(DEFAULT_TOPIC, new HefMQ(DEFAULT_TOPIC));
        topicMQMap.put("aa", new HefMQ("aa"));
    }

    private String topic;
//    private HefMessage<?>[] queue = new HefMessage[1024*10];
    private final HefStore hefStore;

    public HefMQ(String topic) {
        this.topic = topic;
        this.hefStore = new HefStore(topic);
        this.hefStore.init();
    }



    public void subscribe(String topic, String consumerId) {
        subscriptionMap.putIfAbsent(consumerId, new MessageSubscription(topic, consumerId, -1));
    }

    public void unsubscribe(String consumerId) {
        subscriptionMap.remove(consumerId);
    }

    public static void subscribeTopic(String topic, String consumerId) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "Topic not exists: %s", topic);
        hefMQ.subscribe(topic, consumerId);
        log.debug("====> subscribe topic:{}, consumerId: {}", topic, consumerId);
    }

    public static void unsubscribeTopic(String topic, String consumerId) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        hefMQ.unsubscribe(consumerId);
        log.debug("====> unsubscribe topic: {}, consumerId: {}", topic, consumerId);
    }

    public static Integer sendMessage(String topic, HefMessage<?> message) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        int writeIndex = hefMQ.send(message);
        log.debug("====> sendMessage topic:{},writeIndex:{}, message: {}", topic, writeIndex, message);
        return writeIndex;
    }

    public static Integer ackMessage(String topic, String consumerId, Integer offset) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        checkState(Objects.nonNull(subscriptionMap.get(consumerId)), "Please subscribe topic first: %s", topic);
        MessageSubscription messageSubscription = subscriptionMap.get(consumerId);
        if (Objects.nonNull(offset) && offset>messageSubscription.getOffset() && offset<HefStore.LEN) {
            messageSubscription.setOffset(offset);
            log.debug("====> ack message topic:{}, consumerId: {}, offset: {}", topic, consumerId, offset);
            return offset;
        }else {
            return -1;
        }
    }

    public static List<HefMessage<?>> batchReceiveMessage(String topic, String consumerId, Integer size) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        checkState(Objects.nonNull(subscriptionMap.get(consumerId)), "Please subscribe topic first: %s", topic);
        MessageSubscription messageSubscription = subscriptionMap.get(consumerId);
        List<HefMessage<?>> result = Lists.newArrayList();
        for (int i=0, currentOffset = messageSubscription.getOffset(); i<size; i++) {
            Indexer.Entry entry = Indexer.getEntry(topic, currentOffset);
            if (Objects.isNull(entry)) {
                break;
            }
            int nextOffset = currentOffset + entry.getLen();
            HefMessage<?> hefMessage = receiveMessage(topic, consumerId, nextOffset);
            if (Objects.isNull(hefMessage)) {
                break;
            }
            result.add(hefMessage);
            currentOffset = nextOffset;
        }
        return result;
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
        Integer offset = messageSubscription.getOffset();
        int nextOffset = 0;
        if (offset!=-1) {
            Indexer.Entry entry = Indexer.getEntry(topic, offset);
            if (Objects.isNull(entry)) {
                return null;
            }
            nextOffset = offset + entry.getLen();
        }
        return receiveMessage(topic, consumerId, nextOffset);
    }

    public static HefMessage<?> receiveMessage(String topic, String consumerId, Integer offset) {
        HefMQ hefMQ = topicMQMap.get(topic);
        checkState(Objects.nonNull(hefMQ), "topic not exists: %s", topic);
        checkState(Objects.nonNull(subscriptionMap.get(consumerId)), "Please subscribe topic first: %s", topic);
        HefMessage<?> hefMessage = hefMQ.receive(offset);
        log.debug("===> receive topic:{}, consumerId: {}, readIndex: {}, message: {}", topic, consumerId, offset, hefMessage);
        return hefMessage;
    }

    /**
     * 发送消息
     * @param hefMessage
     * @return
     */
    public int send(HefMessage<?> hefMessage) {
        int offset = hefStore.writeOffset();
        hefMessage.getHeaders().put(HefMessage.OFFSET_KEY, String.valueOf(offset));
        hefStore.write(hefMessage);
        return offset;
    }

    /**
     * 获取消息
     * @return
     */
    public HefMessage<?> receive(int offset) {
        return hefStore.read(offset);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(HefMQ.class)
                .add("topic", topic)
                .toString();
    }
}
