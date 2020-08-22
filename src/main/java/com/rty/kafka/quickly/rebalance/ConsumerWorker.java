package com.rty.kafka.quickly.rebalance;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者的多线程，每一个消费者都有一个进程
 *
 * @author rty
 * @since 2020-08-22
 */
public class ConsumerWorker implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    //TODO 用来保存每个消费者当前读取分区的偏移量，解决分区在均衡的关键
    private final Map<TopicPartition, OffsetAndMetadata> currOffsets;
    private final boolean isStop;
    //TODO 事物类可以送入（tr）

    public ConsumerWorker(boolean isStop) {
        Properties properties = KafkaConst.consumerConfig(RebalanceConsumer.GROUP_ID,
                StringDeserializer.class, StringDeserializer.class);
        //TODO 取消自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.isStop = isStop;
        this.consumer = new KafkaConsumer<String, String>(properties);
        this.currOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        //TODO 消费者订阅是加入再均衡监听器(HandlerRebalance）
        consumer.subscribe(Collections.singletonList(BusiConst.REBALANCE_TOPIC), new HandlerRebalance());
    }

    @Override
    public void run() {

    }
}
