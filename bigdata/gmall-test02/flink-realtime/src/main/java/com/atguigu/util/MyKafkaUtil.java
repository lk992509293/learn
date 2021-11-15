package com.atguigu.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String brokers = "hadoop105:9092,hadoop106:9092,hadoop107:9092";

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers, topic, new SimpleStringSchema());
    }

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String group) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }
}
