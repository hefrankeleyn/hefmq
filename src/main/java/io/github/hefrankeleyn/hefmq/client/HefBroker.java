package io.github.hefrankeleyn.hefmq.client;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.github.hefrankeleyn.hefmq.model.HefMessage;
import io.github.hefrankeleyn.hefmq.model.Result;
import io.github.hefrankeleyn.utils.http.HttpInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class HefBroker {

    private static final Logger log = LoggerFactory.getLogger(HefBroker.class);

    private static final String brokerUrl = "http://localhost:8765";
    private static final String SEND = "send";
    private static final String SUBSCRIBE = "subscribe";
    private static final String RECEIVE = "receive";
    private static final String UNSUBSCRIBE = "unsubscribe";
    private static final String ACK = "ack";
    private static final String EMPTY = "";
    private final Gson gson = new Gson();

    public HefProducer createProducer() {
        return new HefProducer(this);
    }

    public HefConsumer createConsumer(String topic) {
        HefConsumer consumer = new HefConsumer(this);
        consumer.subscribe(topic);
        return consumer;
    }

    public boolean send(String topic, HefMessage<String> hefMessage) {
        log.debug("====> send topic: {}, message: {}", topic, hefMessage);
        String url = sendUrl(topic);
        TypeToken<Result<String>> typeToken = new TypeToken<Result<String>>() {};
        Result<String> result = HttpInvoker.httpPost(url, gson.toJson(hefMessage), typeToken);
        log.debug("====> send, result: {}", result);
        return result.isSuccess();
    }

    public boolean subscribe(String topic, String id) {
        log.debug("===> subscribe topic: {}, id: {}", topic, id);
        String url = subscribeUrl(topic, id);
        TypeToken<Result<String>> typeToken = new TypeToken<Result<String>>() {};
        Result<String> result = HttpInvoker.httpPost(url, EMPTY, typeToken);
        log.debug("====> subscribe, result: {}", result);
        return result.isSuccess();
    }

    public boolean unsubscribe(String topic, String id) {
        log.debug("===> unsubscribe topic: {}, id: {}", topic, id);
        String url = unsubscribeUrl(topic, id);
        TypeToken<Result<String>> typeToken = new TypeToken<Result<String>>() {};
        Result<String> result = HttpInvoker.httpPost(url, EMPTY, typeToken);
        log.debug("====> unsubscribe, result: {}", result);
        return result.isSuccess();
    }

    public HefMessage<String> receive(String topic, String id) {
        log.debug("===> receive topic: {}, id: {}", topic, id);
        String url = receiveUrl(topic, id);
        TypeToken<Result<HefMessage<String>>> typeToken = new TypeToken<>() {};
        Result<HefMessage<String>> result = HttpInvoker.httpPost(url, EMPTY, typeToken);
        log.debug("====> receive, result: {}", result);
        return result.getData();
    }

    public boolean ack(String topic, String id, Integer offset) {
        log.debug("===> ack topic: {}, id: {}, offset: {}", topic, id, offset);
        String url = ackUrl(topic, id, offset);
        TypeToken<Result<Integer>> typeToken = new TypeToken<>() {};
        Result<Integer> result = HttpInvoker.httpPost(url, EMPTY, typeToken);
        log.debug("===> ack result: {}", result);
        return result.isSuccess();
    }

    private static String sendUrl(String topic) {
        return createUrl(SEND, topic, null, null);
    }

    private String unsubscribeUrl(String topic, String consumerId) {
        return createUrl(UNSUBSCRIBE, topic, consumerId, null);
    }

    private static String subscribeUrl(String topic, String consumerId) {
        return createUrl(SUBSCRIBE, topic, consumerId, null);
    }

    private static String receiveUrl(String topic, String consumerId) {
        return createUrl(RECEIVE, topic, consumerId, null);
    }

    private static String ackUrl(String topic, String consumerId, Integer offset) {
        return createUrl(ACK, topic, consumerId, offset);
    }


    private static String createUrl(String subPath, String topic, String consumerId, Integer offset) {
        StringBuilder resultUrl = new StringBuilder();
        resultUrl.append(Strings.lenientFormat("%s/hefmq/%s?topic=%s", brokerUrl, subPath, topic));
        if (Objects.nonNull(consumerId) && !consumerId.isBlank()) {
            resultUrl.append(Strings.lenientFormat("&consumerId=%s", consumerId));
        }
        if (Objects.nonNull(offset)) {
            resultUrl.append(Strings.lenientFormat("&offset=%s", offset));
        }
        return resultUrl.toString();
    }



}
