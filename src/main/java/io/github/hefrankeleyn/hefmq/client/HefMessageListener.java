package io.github.hefrankeleyn.hefmq.client;

import io.github.hefrankeleyn.hefmq.model.HefMessage;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public interface HefMessageListener {

    void onMessage(HefMessage<?> hefMessage);
}
