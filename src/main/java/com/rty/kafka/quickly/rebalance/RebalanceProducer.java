package com.rty.kafka.quickly.rebalance;

import com.rty.kafka.quickly.config.BusiConst;
import com.rty.kafka.quickly.config.KafkaConst;
import com.rty.kafka.quickly.vo.DemoUser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 多線程使用下生产者
 */
public class RebalanceProducer {
    private static final int MSG_SIZE=50;
    private static ExecutorService executorService= Executors.newFixedThreadPool(Runtime.getRuntime().
            availableProcessors());
    private static CountDownLatch countDownLatch=new CountDownLatch(MSG_SIZE);

    private static DemoUser makeUser(int id){
        DemoUser demoUser=new DemoUser(id);
        String userName="xingxue_"+id;
        demoUser.setName(userName);
        return  demoUser;
    }
    private static class ProduceWorker implements Runnable{
        private ProducerRecord<String,String> record;
        private KafkaProducer<String,String> producer;
        public ProduceWorker(ProducerRecord<String,String> record,KafkaProducer<String,String> producer){
            this.record=record;
            this.producer=producer;
        }
        @Override
        public void run() {
            final String id=Thread.currentThread().getId()+"-"+System.identityHashCode(producer);
            try{
                producer.send(record,(recordMetadata,e)->{
                    if(null!=e){
                        e.printStackTrace();
                    }
                    if(null!=recordMetadata){
                        System.out.println("offet:" + recordMetadata.offset() + "-" + "partition:" +
                                recordMetadata.partition() + "-" + "topic:" + recordMetadata.topic());
                    }
                    System.out.println(id+"数据["+record+"]已发送");
                    countDownLatch.countDown();
                });
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(KafkaConst.
                producerConfig(StringSerializer.class,StringSerializer.class));
        try{
            for(int i=0;i<MSG_SIZE;i++){
                DemoUser demoUser=makeUser(i);
                ProducerRecord<String,String> record=new ProducerRecord<>(BusiConst.REBALANCE_TOPIC,null,
                        System.currentTimeMillis(),demoUser.getId()+"",demoUser.toString());
                executorService.submit(new ProduceWorker(record,producer));
                Thread.sleep(600);
            }
            countDownLatch.wait();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
            executorService.shutdown();
        }
    }
}
