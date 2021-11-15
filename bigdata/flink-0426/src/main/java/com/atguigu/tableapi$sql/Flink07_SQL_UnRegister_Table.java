package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink07_SQL_UnRegister_Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //转流为表
        Table table = tableEnvironment.fromDataStream(dataStreamSource);

/*        //方式1：将表转为流
        //查询数据
        Table sqlQuery = tableEnvironment.sqlQuery("select * from " + table + " where id='sensor_1'");

        DataStream<Row> result = tableEnvironment.toAppendStream(sqlQuery, Row.class);
        result.print();
        env.execute();
        */

/*      //方式2：直接使用excute执行
         //查询数据
        Table sqlQuery = tableEnvironment.sqlQuery("select * from " + table + " where id='sensor_1'");

        TableResult result = sqlQuery.execute();
        result.print();*/

        //方式三：使用excutesql执行sql语句
        tableEnvironment.executeSql("select * from " + table + " where id='sensor_1'").print();

    }
}
