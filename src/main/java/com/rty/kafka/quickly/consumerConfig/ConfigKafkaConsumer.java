package com.rty.kafka.quickly.consumerConfig;

import com.rty.kafka.quickly.config.BusiConst;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * kafka的配置
 *
 * @author rty
 * @since 2020-08-18
 */
public class ConfigKafkaConsumer {

    public static void main(String[] args) {
        //TODO 消费者三个属性必须制定(boeker地址清单,key和value的反序列化器)
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        //TODO 群组并非完全必要
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");
        //TODO 配置更多的消费者配置(重要的)
        properties.put("auto.offset.reset", "latest");//消费者在读取一个没有偏移量的分区或者偏移量无效的情况下，如何处理
        properties.put("enable.auto.commit", true);//表明消费者是否是自动提交偏移量，默认为true
        properties.put("max.poll.records", 500);//控制每次pull方法返回的记录数量，默认为500
        //分区分配给消费者的策略，系统提供两种策略，默认为Range
        properties.put("partition.assignment.strategy", Collections.singletonList(RangeAssignor.class));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        try {
            //TODO 消费者订阅主题(可以多个)
            consumer.subscribe(Collections.singletonList(BusiConst.HELLO_TOPIC));
            while (true) {
                //TODO 拉取(新版本)
                ConsumerRecords<String, String> records = consumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic:%s,分区,%d,偏移量,%d" + "key:%s,value:%s", record.topic(), record.partition(),
                            record.offset(), record.key(), record.value()));
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
