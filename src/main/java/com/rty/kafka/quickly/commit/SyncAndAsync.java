package com.rty.kafka.quickly.commit;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * kafka的comsumer同步,异步提交
 *
 * @author rty
 * @since 2020-08-21
 */
public class SyncAndAsync {
    public static void main(String[] args) {
        Properties properties= KafkaConst.consumerConfig("async", StringDeserializer.class,StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        try{
            consumer.subscribe(Collections.singletonList(BusiConst.CONCURRENT_USER_INFO_TOPIC));
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println( String.format("主题:%s,分区:%d,偏移量:%d" +
                                    "key:%s,value:%s", record.topic(), record.partition(), record.offset(),
                            record.key(), record.value()));
                    //do our work
                }
                //异步提交偏移量
                consumer.commitAsync();
                consumer.commitAsync((Map<TopicPartition, OffsetAndMetadata> offsets, Exception e)->{
                    if(e!=null){
                        System.out.println("commit failed for offsets");
                        System.out.println(offsets);
                        e.printStackTrace();
                    }
                });

            }

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            consumer.commitSync();
            consumer.close();
        }
    }
}
