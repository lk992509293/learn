package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;


public class Flink01_TableApi_BasicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //TODO 1.创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2.创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取
        Table table = tableEnvironment.fromDataStream(waterSensorDataSource);

        //TODO 3.对动态进行查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //TODO 4.将表再转为流
        DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(resultTable, Row.class);

        //打印输出
        rowDataStream.print();

        env.execute();

    }
}
