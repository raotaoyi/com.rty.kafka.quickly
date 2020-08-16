package com.rty.kafka.quickly.producerConfig;

import com.rty.kafka.quickly.config.BusiConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 数据顺序保证的kafka的producer
 *
 * @author rty
 * @since 2020-08-26
 */
public class OrderKafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //TODO 顺序消息的保证(只有一个分区)
        properties.put("max.in.flight.requests.per.connection",1);//设置为1，这样生产者发送第一批数据时，就不会有其他的消息发送给broker
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        //发送的记录
        ProducerRecord<String,String> record;
        try{
            record=new ProducerRecord<String, String>(BusiConst.HELLO_TOPIC,null,"helloworld");
            producer.send(record); //发送即忘记,
            System.out.println("kafka message is send");
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}
