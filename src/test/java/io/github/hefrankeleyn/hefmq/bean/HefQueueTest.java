package io.github.hefrankeleyn.hefmq.bean;


import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    @Test
    public void test02() {
        try {
            BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<>();
            new Thread(()->{
                try {
                    Thread.sleep(1000 * 10L);
                    blockingQueue.put("aa");
                }catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();
            System.out.println("====> begin...");
            blockingQueue.take();
            String res = blockingQueue.take();
            System.out.println("====> ok: " + res);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
