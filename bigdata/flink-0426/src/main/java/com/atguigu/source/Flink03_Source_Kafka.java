package com.atguigu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop105:9092,hadoop106:9092,hadoop107:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> sensorStream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));

        //3.输出并打印
        sensorStream.print();

        //4.开启执行
        env.execute();
    }
}
