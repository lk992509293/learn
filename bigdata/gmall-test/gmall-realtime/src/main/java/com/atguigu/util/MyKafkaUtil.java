package com.atguigu.util;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String broker = "hadoop105:9092,hadoop106:9092,hadoop107:9092";

    //获取消费者对象
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    //获取生产者对象
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(broker, topic, new SimpleStringSchema());
    }

    //自定义序列化方式获取生产者对象
    public static <T> FlinkKafkaProducer<T> getFlinkKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        return new <T>FlinkKafkaProducer<T>("DWD_DEFAULT_TOPIC", kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
