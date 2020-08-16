package com.rty.kafka.quickly.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka的配置
 */
public class KafkaConst {
    public static String LOCAL_BROKER = "127.0.0.1:9092";
    public static String BROKER_LIST = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";

    public static Properties producerConfig(
            Class<StringSerializer> keySerializableClazz,
            Class<StringSerializer> valueSerializableClazz
    ) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCAL_BROKER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializableClazz);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializableClazz);
        return properties;
    }
}
