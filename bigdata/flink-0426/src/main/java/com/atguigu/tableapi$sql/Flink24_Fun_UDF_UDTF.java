package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

public class Flink24_Fun_UDF_UDTF {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
//        SingleOutputStreamOperator<WaterSensor> waterSensorDataStreamSource = env.socketTextStream("hadoop105", 9999)
//                .map(new MapFunction<String, WaterSensor>() {
//                    @Override
//                    public WaterSensor map(String value) throws Exception {
//                        String[] split = value.split(",");
//                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
//                    }
//                });

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //不注册直接使用自定义函数
//        table
//                .joinLateral(call(MyUDTF.class, $("id")))
//                .select($("id"), $("word"))
//                .execute()
//                .print();

        //注册一个自定义函数
        tableEnv.createTemporarySystemFunction("MyUdtf", MyUDTF.class);

//        TableAPI
//                table
//                .joinLateral(call("MyUdtf", $("id")))
//                .select($("id"), $("word"))
//                .execute()
//                .print();

        //SQL
//        tableEnv.executeSql("select word,id from "+table+", LATERAL TABLE(MyUdtf(id))").print();
        tableEnv.executeSql("select word,id from "+table+" join LATERAL TABLE(MyUdtf(id)) on True").print();
    }

    //自定义一个UDTF函数，将传进来id按照"_"切分
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))//注解可以提供从入参数据类型到结果数据类型的映射，提供输出字段名和类型
    public static class MyUDTF extends TableFunction<Row>{

        public void eval(String value){
            String[] words = value.split("_");
            for (String word : words) {
                collect(Row.of(word));
            }
        }
    }

//    public static class MyFlatMap implements FlatMapFunction<String,String>{
//
//        @Override
//        public void flatMap(String value, Collector<String> out) throws Exception {
//            String[] words = value.split(" ");
//            for (String word : words) {
//                out.collect(word);
//            }
//        }
//    }
}
