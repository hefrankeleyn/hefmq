package io.github.hefrankeleyn.hefmq.bean;


import org.junit.Test;

import java.util.Queue;

/**
 * @Date 2024/7/18
 * @Author lifei
 */
public class HefQueueTest {

    @Test
    public void testHefQueue() {
        HefQueue<String> hefQueue = new HefQueue<>();
        hefQueue.enqueue("aa");
        hefQueue.enqueue("bb");
        hefQueue.enqueue("cc");
        hefQueue.enqueue("dd");

        for (String item : hefQueue) {
            System.out.println(item);
        }
    }

}
