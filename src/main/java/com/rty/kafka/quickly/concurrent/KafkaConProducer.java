package com.rty.kafka.quickly.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 生产者在多线程下,因为生产者是线性安全的，往kafka中发送，相当于往队列中发送数据
 *
 * @author rty
 * @since 2020-08-16
 */
public class KafkaConProducer {
    // 生产消息的条数
    private static final int MSG_SIZE = 1000;
    // 负责线程的线程池
    private static ExecutorService executorService = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors());
    private static CountDownLatch countDownLatch = new CountDownLatch(MSG_SIZE);

}
