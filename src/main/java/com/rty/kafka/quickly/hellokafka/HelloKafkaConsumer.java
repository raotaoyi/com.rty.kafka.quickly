package com.rty.kafka.quickly.hellokafka;

import com.rty.kafka.quickly.config.BusiConst;
import javafx.util.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * kafka的消费者
 *
 * @author rty
 * @since 2020-12-16
 */
public class HelloKafkaConsumer {
    public static void main(String[] args) {
        //TODO 消费者三个属性必须制定(boeker地址清单,key和value的反序列化器)
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);

        //TODO 群组并非完全必要
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        try {
            //TODO 消費者订阅主题(可以多个)
            consumer.subscribe(Collections.singletonList(BusiConst.HELLO_TOPIC));
            while (true) {
                //TODO 拉取(新版本)
                ConsumerRecords<String, String> records = consumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic:%s,分区,%d,偏移量,%d"+"key:%s,value:%s",record.topic(),record.partition(),
                            record.offset(),record.key(),record.value()));

                    //do my work
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
