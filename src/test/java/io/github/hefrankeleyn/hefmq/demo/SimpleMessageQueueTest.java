package io.github.hefrankeleyn.hefmq.demo;

import io.github.hefrankeleyn.hefmq.bean.Order;
import io.github.hefrankeleyn.hefmq.core.HefBroker;
import io.github.hefrankeleyn.hefmq.core.HefConsumer;
import io.github.hefrankeleyn.hefmq.core.HefMessage;
import io.github.hefrankeleyn.hefmq.core.HefProducer;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class SimpleMessageQueueTest {

    private static final AtomicLong counter = new AtomicLong(0);

    @Test
    public void test01() {
        String topic = "demo-01";
        HefBroker hefBroker = new HefBroker();
        hefBroker.createTopic(topic);

        // 生产者
        HefProducer producer = hefBroker.createProducer();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                producer.send(topic, new HefMessage<>(counter.getAndIncrement(),
                        new Order((long) i, "aa" + i, 10.2 * i)));
            }
        }).start();


        // 消费者
        HefConsumer consumer = hefBroker.createConsumer(topic);

        consumer.addMessageListener(hefMessage -> {
            System.out.println("===> 监听到有一条消息： " + hefMessage.getBody());
        });
        while (true) {
            try {
                char c = (char) System.in.read();
                if (c == 'q' || c == 'e') {
                    break;
                } else if (c == 'p') {
                    long num = counter.get();
                    producer.send(topic, new HefMessage<>(counter.getAndIncrement(),
                            new Order(num, "aa" + num, 10.2 * num)));
                } else if (c == 'c') {
                    HefMessage<?> oneMessage = consumer.poll(1000L);
                    if (Objects.nonNull(oneMessage)) {
                        HefMessage<Order> orderMessage = (HefMessage<Order>) oneMessage;
                        System.out.println(orderMessage.getBody());
                    } else {
                        System.out.println("===> 目前没有消息可消费。");
                    }
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
