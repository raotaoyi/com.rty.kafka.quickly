package com.rty.kafka.quickly.concurrent;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import com.rty.kafka.quickly.vo.DemoUser;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

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

    public static DemoUser makeUser(int id) {
        DemoUser demoUser = new DemoUser(id);
        String userName = "xingxue_" + id;
        demoUser.setName(userName);
        return demoUser;
    }

    /**
     * 发送消息的任务
     */
    private static class ProducerWork implements Runnable {
        private KafkaProducer<String, String> producer;
        private ProducerRecord<String, String> record;

        public ProducerWork(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
            this.producer = producer;
            this.record = record;
        }

        public void run() {
            final String id=Thread.currentThread().getId()+"-"+System.identityHashCode(producer);
            try{
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (null != e) {
                            e.printStackTrace();
                        }
                        if (null != recordMetadata) {
                            System.out.println("offet:" + recordMetadata.offset() + "-" + "partition:" +
                                    recordMetadata.partition() + "-" + "topic:" + recordMetadata.topic());
                        }
                    }
                });
                System.out.println(id+":数据["+record+"]已发送.");
                countDownLatch.countDown();

            }catch (Exception e){
                e.printStackTrace();
            }
      }
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaConst.
                producerConfig(StringSerializer.class, StringSerializer.class));
        try {
            //循环发送，通过线程池的方式
            for (int i = 0; i < MSG_SIZE; i++) {
                DemoUser demoUser = makeUser(i);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(BusiConst.CONCURRENT_USER_INFO_TOPIC,
                        null, System.currentTimeMillis(), demoUser.getId() + "", demoUser.toString());
                executorService.submit(new ProducerWork(producer, record));
            }
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
