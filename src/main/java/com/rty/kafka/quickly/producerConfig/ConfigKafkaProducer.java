package com.rty.kafka.quickly.producerConfig;

import com.rty.kafka.quickly.config.BusiConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafa生产者的配置
 *
 * @author raotaoyi
 * @since 2020-08-09 22:21
 */
public class ConfigKafkaProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //重要的
        properties.put("ack", 1);//ack:0,1,all 数据确认机制.0:数据不用确认，1，leader副本确认，all，全部副本确认
        properties.put("batch.size", 16384);//一次批次可以使用的内存大小，缺省16384(16k)
        properties.put("linger.ms", 0L);//指定了生产者在发送批次前等待更多的消息加入批次的时间，缺省50毫秒
        properties.put("max.request.size", 1 * 1024 * 1024);//控制生产者发送请求量的大小，默认1M，(这个参数和kafka中的配置相关)
        //非重要的
        properties.put("buffer.memory",32*1024*1024);//生产者内存缓冲区的大小
        properties.put("retries",Integer.MAX_VALUE);//重发的次数，特殊情况下，不会重发(发送的请求数量太大，超过kafka设置的大小)
        properties.put("request.timeout.ms",30*1000);//客户端将等待请求的响应的最大时间，默认30秒
        properties.put("compression","none");//干压缩数据的压缩类型，默认为无压缩，none，gzip，snappy

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
