package io.github.hefrankeleyn.hefmq.client;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import io.github.hefrankeleyn.hefmq.model.HefMessage;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class HefMessageQueue {

    private String topic;
    private BlockingQueue<HefMessage<?>> queue = new LinkedBlockingQueue<>();

    private List<HefMessageListener> listeners = Lists.newArrayList();
    public HefMessageQueue() {}

    public HefMessageQueue(String topic) {
        this.topic = topic;
    }


    public boolean send(HefMessage<?> hefMessage) {
        boolean ok = queue.offer(hefMessage);
        if (ok && Objects.nonNull(listeners)) {
            for (HefMessageListener listener : listeners) {
                listener.onMessage(hefMessage);
            }
        }
        return ok;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public HefMessage<?> poll(long timeout) {
        try {
            return queue.poll(timeout, TimeUnit.MILLISECONDS);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(HefMessageQueue.class)
                .add("topic", topic)
                .toString();
    }


    public void listener(HefMessageListener hefMessageListener) {
        listeners.add(hefMessageListener);
    }
}
