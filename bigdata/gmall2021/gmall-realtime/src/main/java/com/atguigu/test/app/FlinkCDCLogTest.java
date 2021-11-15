package com.atguigu.test.app;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkCDCLogTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka的ods_log读取数据，该路径经由日志服务器罗盘到文件，然后由flume采集再发送到kafka的ods_log主题
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop105:9092," +
                "hadoop106:9092,hadoop107:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ods_log");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("ods_log", new SimpleStringSchema(), properties));

        //打印日志
        dataStreamSource.print("ods_log>>>>");

        env.execute();
    }
}
