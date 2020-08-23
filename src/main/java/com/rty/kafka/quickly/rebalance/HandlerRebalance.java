package com.rty.kafka.quickly.rebalance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HandlerRebalance implements ConsumerRebalanceListener {
    //模拟一个保存分区偏移量的的数据库表
    public final static ConcurrentHashMap<TopicPartition,Long> partitionOffsetMap=new ConcurrentHashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> currOffsets;
    private final KafkaConsumer<String,String> consumer;
    public HandlerRebalance(Map<TopicPartition,OffsetAndMetadata> currOffsets,KafkaConsumer<String,String> consumer){
        this.currOffsets=currOffsets;
        this.consumer=consumer;
    }
    //分区再均衡之前
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        final String id=Thread.currentThread().getId()+"";
        System.out.println(id+"-onPartitionsRevoked参数值为:"+partitions);
        System.out.println(id+"-服务器准备分区在均衡,提交偏移量，当前的偏移量为:"+currOffsets);
        //我们可以不使用consumer.commitSync(currOffsets);
        //提交偏移量到kafka,由我们自己维护
        //开始事务
        //偏移量写入数据库
        System.out.println("分区偏移量表中:"+partitionOffsetMap);
        partitions.parallelStream().forEach((topic)->{
            partitionOffsetMap.put(topic,currOffsets.get(topic).offset());
        });
        consumer.commitSync(currOffsets);
        //提交业务数和偏移量入库 tr.commit
        
    }

    //分区再均衡完成后
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        final String id=Thread.currentThread().getId()+"";
        System.out.println(id+"-再均衡完成,onPartitionsAssigned参数为:"+partitions);
        System.out.println("分区偏移量表中:"+partitionOffsetMap);
        for(TopicPartition topicPartition:partitions){
            System.out.println(id+"-topicPartition"+topicPartition);
            Long offSet=partitionOffsetMap.get(topicPartition);
            if(offSet==null) {
                continue;
            }
            //TODO 从指定偏移量处开始记录(从指定分区中的指定偏移量开始消费)
            //TODO 这样就可以确保分区再均衡中的数据不错乱
            consumer.seek(topicPartition,partitionOffsetMap.get(topicPartition));
        };

    }
}
