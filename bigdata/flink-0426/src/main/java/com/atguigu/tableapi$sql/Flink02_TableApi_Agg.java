package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink02_TableApi_Agg {
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

        //TODO 2.将流转换为表
        Table table = tableEnvironment.fromDataStream(waterSensorDataSource);

        //TODO 3.对表执行查询操作
        Table resultTable = table
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("vcSum"));

        //TODO 4.将表转换为流
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnvironment.toRetractStream(resultTable, Row.class);

        //打印输出结果
        tuple2DataStream.print();

        env.execute();
    }
}
