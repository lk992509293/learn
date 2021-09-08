package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) throws InterruptedException {
        //1.创建kafka配置对象
        Properties properties = new Properties();

        //2.给kafka添加配置信息
        properties.put("bootstrap.servers", "hadoop102:9092");

        //设置批次大小
        properties.put("batch.size", 16384);

        //设置等待时间
        properties.put("linger.ms", 1);

        //设置缓冲区默认大小
        properties.put("buffer.memory", 33554432);

        //设置ack
        properties.put("acks", "all");

        //设置重试次数
        properties.put("retries", 3);

        //key，value序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //添加自定义分区器
        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.kafka.producer.MyPartitioner");

        //3.创建kafka对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //4.调用send发送消息
        for (int i = 0; i < 100; i++) {
            // kafkaProducer.send(new ProducerRecord<String, String>("first", "kafka" + i));
            //使用带回调函数的send
            kafkaProducer.send(new ProducerRecord<>("first",  "kafka" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("success->" + recordMetadata.partition());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
            Thread.sleep(5);
        }

        //5.关闭资源
        kafkaProducer.close();
    }
}
