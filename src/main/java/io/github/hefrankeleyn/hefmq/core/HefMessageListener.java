package io.github.hefrankeleyn.hefmq.core;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public interface HefMessageListener {

    void onMessage(HefMessage<?> hefMessage);
}
