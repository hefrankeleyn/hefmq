package io.github.hefrankeleyn.hefmq.server;


import io.github.hefrankeleyn.hefmq.model.HefMessage;
import io.github.hefrankeleyn.hefmq.model.Result;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Objects;

/**
 * @Date 2024/7/22
 * @Author lifei
 */
@RestController
@RequestMapping(value = "/hefmq")
public class HefMQServer {

    // send
    @RequestMapping(value = "/send", method = RequestMethod.POST)
    public Result<Integer> send(@RequestParam("topic") String topic,
                                @RequestBody HefMessage<String> hefMessage) {
        return Result.ok(HefMQ.sendMessage(topic, hefMessage));
    }

    // receive
    @RequestMapping(value = "/receive", method = RequestMethod.POST)
    public Result<HefMessage<?>> receive(String topic, String consumerId) {
        return Result.ok(HefMQ.receiveMessage(topic, consumerId));
    }

    // batchReceive
    @RequestMapping(value = "/batchReceive", method = RequestMethod.POST)
    public Result<List<HefMessage<?>>> batchReceive(String topic, String consumerId, Integer size) {
        if (Objects.isNull(size) || size<=0) {
            size = 100;
        }
        return Result.ok(HefMQ.batchReceiveMessage(topic, consumerId, size));
    }

    // subscribe
    @RequestMapping(value = "/subscribe", method = RequestMethod.POST)
    public Result<String> subscribe(@RequestParam("topic") String topic,
                                    @RequestParam("consumerId") String consumerId) {
        HefMQ.subscribeTopic(topic, consumerId);
        return Result.ok(Result.CodeEnum.SUCCESS.name());
    }

    // unsubscribe
    @RequestMapping(value = "/unsubscribe", method = RequestMethod.POST)
    public Result<String> unsubscribe(@RequestParam("topic") String topic,
                                      @RequestParam("consumerId") String consumerId) {
        HefMQ.unsubscribeTopic(topic, consumerId);
        return Result.ok(Result.CodeEnum.SUCCESS.name());
    }

    // ack
    @RequestMapping(value = "/ack", method = RequestMethod.POST)
    public Result<Integer> ack(@RequestParam("topic") String topic,
                               @RequestParam("consumerId") String consumerId,
                               @RequestParam("offset") Integer offset) {
        return Result.ok(HefMQ.ackMessage(topic, consumerId, offset));
    }

}
