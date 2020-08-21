package com.rty.kafka.quickly.commit;

import com.rty.kafka.quickly.config.KafkaConst;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;

/**
 * kafka的comsumer同步提交
 *
 * @author rty
 * @since 2020-08-21
 */
public class CommitSync {
    public static void main(String[] args) {
        Properties properties= KafkaConst.consumerConfig("async", StringDeserializer.class,StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        try{
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println( String.format("主题:%s,分区:%d,偏移量:%d" +
                                    "key:%s,value:%s", record.topic(), record.partition(), record.offset(),
                            record.key(), record.value()));
                    //do our work
                }
                //k开始事务
                //读业务写数据库
                //偏移量写入数据库
                //同步提交偏移量
                consumer.commitSync();
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }
}
