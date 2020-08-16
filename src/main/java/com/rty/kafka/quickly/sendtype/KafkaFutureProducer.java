package com.rty.kafka.quickly.sendtype;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.Future;

/**
 * kafka三种发送方式之一---同步(容易造成阻塞,生产者一直没有关闭，被占用，影响性能)
 *
 * @author rty
 * @since 2020-08-16
 */
public class KafkaFutureProducer {

    private static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) {
        producer = new KafkaProducer<String, String>(KafkaConst.producerConfig(StringSerializer.class, StringSerializer.class));
        ProducerRecord<String, String> record = null;
        try {
            record = new ProducerRecord<String, String>(BusiConst.HELLO_TOPIC, "teacher10", "jack");
            Future<RecordMetadata> future = producer.send(record);
            System.out.println("do other thing");
            RecordMetadata recordMetadata = future.get(); //易发生阻塞
            if (null != recordMetadata) {
                System.out.println("offet:" + recordMetadata.offset() + "-" + "partition:" + recordMetadata.partition() + "-" + "topic:" + recordMetadata.topic());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
