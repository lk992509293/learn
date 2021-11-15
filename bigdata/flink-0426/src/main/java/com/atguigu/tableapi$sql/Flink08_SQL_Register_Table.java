package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//查询已注册的表
public class Flink08_SQL_Register_Table {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //创建表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //4.将流转为表(未注册的表)
//        Table table = tableEnvironment.fromDataStream(waterSensorDataStream);

        //5.对未注册的表进行注册创建一个临时视图表
//        tableEnv.createTemporaryView("sensor", table);

        //直接对表注册，创建临时视图
        tableEnvironment.createTemporaryView("sensor", waterSensorDataStream);

        //如果需要table对象，可以从表名获取
//        Table table = tableEnvironment.from("sensor");

        //直接对表的环境执行sql操作，返回一个TableResult对象，然后直接打印
        tableEnvironment.executeSql("select * from sensor where id='sensor_1'").print();
    }
}
