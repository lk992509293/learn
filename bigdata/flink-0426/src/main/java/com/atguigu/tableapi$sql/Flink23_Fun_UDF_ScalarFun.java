package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink23_Fun_UDF_ScalarFun {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //创建表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //将流转为动态表
        Table table = tableEnvironment.fromDataStream(dataStreamSource);

        //注册一个自定函数
        tableEnvironment.createTemporarySystemFunction("MyUDF", MyUDF.class);

        //不注册直接使用自定义函数
//        table.select(call(MyUDF.class, $("id")), $("id"))
//                .execute()
//                .print();

        //TableAPI
        table.select(call("MyUDF", $("id")), $("id"))
                .execute()
                .print();

        //执行SQL
//        tableEnvironment.executeSql("select MyUDF(id) from " + table).print();
    }

    //自定义一个UDF函数，用来求某个字段的长度
    public static class MyUDF extends ScalarFunction {
        public int eval(String value) {
            return value.length();
        }
    }
}
