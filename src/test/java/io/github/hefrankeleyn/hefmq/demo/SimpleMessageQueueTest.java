package io.github.hefrankeleyn.hefmq.demo;

import com.google.gson.Gson;
import io.github.hefrankeleyn.hefmq.bean.Order;
import io.github.hefrankeleyn.hefmq.client.HefBroker;
import io.github.hefrankeleyn.hefmq.client.HefConsumer;
import io.github.hefrankeleyn.hefmq.model.HefMessage;
import io.github.hefrankeleyn.hefmq.client.HefProducer;
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
        String topic = "com.hef.demo";
        HefBroker hefBroker = new HefBroker();

        // 生产者
        HefProducer producer = hefBroker.createProducer();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                Order order = new Order((long) i, "aa" + i, 10.2 * i);
                producer.send(topic, new HefMessage<>(counter.getAndIncrement(), new Gson().toJson(order)));
            }
        }).start();


        // 消费者
        HefConsumer consumer = hefBroker.createConsumer(topic);

//        consumer.addMessageListener(hefMessage -> {
//            System.out.println("===> 监听到有一条消息： " + hefMessage.getBody());
//        });
        while (true) {
            try {
                char c = (char) System.in.read();
                if (c == 'q' || c == 'e') {
                    consumer.unsubscribe();
                    break;
                } else if (c == 'p') {
                    long num = counter.get();
                    Order order = new Order(num, "aa" + num, 10.2 * num);
                    producer.send(topic, new HefMessage<>(counter.getAndIncrement(), new Gson().toJson(order)));
                } else if (c == 'c') {
                    HefMessage<?> oneMessage = consumer.receive();
                    consumer.ackMessage(oneMessage);
                    if (Objects.nonNull(oneMessage)) {
                        HefMessage<String> orderMessage = (HefMessage<String>) oneMessage;
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
