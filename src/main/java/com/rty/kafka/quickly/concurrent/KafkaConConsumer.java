package com.rty.kafka.quickly.concurrent;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 多线程的消费者
 * 同一个消费者多个消费跑，容易造成数据重复消费等问题
 *
 * @author rty
 * @since 2020-08-18
 */
public class KafkaConConsumer {

    private static ExecutorService executorService =
            Executors.newFixedThreadPool(BusiConst.CONCURRENT_PATITIONS_COUNT);

    public static class ConsumerWorker implements Runnable {

        private KafkaConsumer<String, String> consumer;

        //TODO 使用KafkaConsumer的實例要小心，應該每一個消費者數據線程都有自己的kafkaConsumer實例

        public ConsumerWorker(Map<String, Object> config, String topic) {
            Properties properties = new Properties();
            properties.putAll(config);
            this.consumer = new KafkaConsumer<String, String>(properties);
            //这个方法主要用于只有一个元素的优化，减少内存分配，无需分配额外的内存，可以从SingletonList内部类看得出来,
            // 由于只有一个element,因此可以做到内存分配最小化，相比之下ArrayList的DEFAULT_CAPACITY=10个
            consumer.subscribe(Collections.singletonList(topic));
        }

        public void run() {
            final String id = Thread.currentThread().getId() + "-" + System.identityHashCode(consumer);
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(500);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(id + "|" + String.format("主题:%s,分区:%d,偏移量:%d" +
                                        "key:%s,value:%s", record.topic(), record.partition(), record.offset(),
                                record.key(), record.value()));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        //消费配置的实例
        Map<String, Object> config = KafkaConst.consumerConfigMap("concurrent",
                StringDeserializer.class, StringDeserializer.class);
        for (int i = 0; i < BusiConst.CONCURRENT_PATITIONS_COUNT; i++) {
            executorService.submit(new ConsumerWorker(config, BusiConst.CONCURRENT_USER_INFO_TOPIC));
        }
    }
}

