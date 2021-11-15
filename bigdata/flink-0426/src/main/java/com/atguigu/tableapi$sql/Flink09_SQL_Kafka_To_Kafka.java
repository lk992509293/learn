package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink09_SQL_Kafka_To_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //创建从kafka消费数据
        tableEnvironment.executeSql(
                "CREATE TABLE source_sensor (id string, ts bigint, vc int) WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'topic_source_sensor',\n" +
                        "  'properties.bootstrap.servers' = 'hadoop105:9092,hadoop106:9092,hadoop107:9092',\n" +
                        "  'properties.group.id' = 'sensor',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'csv'\n" +
                        ")");

        //创建写数据到kafka的表
        tableEnvironment.executeSql(
                "CREATE TABLE sink_sensor (id string, ts bigint, vc int) WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'topic_sink_sensor',\n" +
                        "  'properties.bootstrap.servers' = 'hadoop105:9092,hadoop106:9092,hadoop107:9092',\n" +
                        "  'format' = 'csv'\n" +
                        ")");

        //开始执行sql语句
        tableEnvironment.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");
    }
}
