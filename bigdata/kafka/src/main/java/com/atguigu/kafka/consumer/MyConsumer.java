package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class MyConsumer{
    public static void main(String[] args) {
        //1.创建消费者配置对象
        Properties properties = new Properties();

        //2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        //3.配置key，value序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //4.配置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        //5.创建消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //6.注册主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("first");
        kafkaConsumer.subscribe(strings);

        //7.拉取数据打印
        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }
        }
    }
}
