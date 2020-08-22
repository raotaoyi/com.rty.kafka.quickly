package com.rty.kafka.quickly.rebalance;

import com.rty.kafka.quickly.config.BusiConst;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 再均衡消费
 *
 * @author rty
 * @since 2020-08-22
 */
public class RebalanceConsumer {

    public static final String GROUP_ID = "rebalanceconsumer";

    //TODO 使用线程池，两个线程(两个消费者)
    private static ExecutorService executorService =
            Executors.newFixedThreadPool(BusiConst.CONCURRENT_PATITIONS_COUNT);

    public static void main(String[] args) throws InterruptedException {
        //TODO 先起两个消费着，消费3个分区
        for (int i = 0; i < BusiConst.CONCURRENT_PATITIONS_COUNT; i++) {
            executorService.submit(new ConsumerWorker(false));
        }
        Thread.sleep(5000);
        //用来被停止，观察保持运行的消费者
        // TODO 步骤2,再起一个消费者，这个消费者消费了5条数据后再关闭
        //TODO 这里会触发两次的分区再均衡，一次是消费者的加入，另一次是这个消费者消费一段后离开
        new Thread(new ConsumerWorker(true)).start();
    }
}
