package com.rty.kafka.quickly.rebalance;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import org.apache.kafka.clients.consumer.*;
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
        consumer.subscribe(Collections.singletonList(BusiConst.REBALANCE_TOPIC),
                new HandlerRebalance(this.currOffsets,this.consumer));
    }

    @Override
    public void run() {
        final String id=Thread.currentThread().getId()+"";
        int count=0;
        TopicPartition topicPartition=null;
        long offSet=0;
        try{
            while(true){
                ConsumerRecords<String,String> records=consumer.poll(500);
                //TODO 业务处理
                for(ConsumerRecord<String,String> record:records){
                    System.out.println(id + "|" + String.format("主题:%s,分区:%d,偏移量:%d" +
                                    "key:%s,value:%s", record.topic(), record.partition(), record.offset(),
                            record.key(), record.value()));
                    topicPartition=new TopicPartition(record.topic(),record.partition());
                    offSet=record.offset()+1;
                    //TODO 消費者消費時把偏移量提交到統一的hashmap中
                    currOffsets.put(topicPartition,new OffsetAndMetadata(offSet,"no"));
                    count++;
                    //執行業務的sql
                }
                //TODO 提交事務
                //提交業務數和偏移量入庫 tr.commit
                //TODO 如果stop的參數為true，這個消費者消費到第5個時自動關閉
                if(isStop && count>=5){
                    System.out.println(id+"-将关闭，当前的偏移量为:"+currOffsets);
                    consumer.commitSync();
                    break;
                }
                consumer.commitSync();
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }
}
