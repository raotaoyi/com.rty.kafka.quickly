package com.rty.kafka.quickly.sendtype;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * kafka的三种发送方式之一--异步发送(推荐)
 */
public class KafkaAsynProducer {
    private static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) {
        producer = new KafkaProducer<String, String>(KafkaConst.producerConfig(StringSerializer.class,
                StringSerializer.class));
        ProducerRecord<String, String> record = null;
        try {
            record = new ProducerRecord<String, String>(BusiConst.HELLO_TOPIC, "teacher14", "rose");
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }


    }
}
