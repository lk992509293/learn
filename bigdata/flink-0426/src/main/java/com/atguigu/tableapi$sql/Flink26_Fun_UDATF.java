package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink26_Fun_UDATF {
    public static void main(String[] args) {
        //获取流和表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //2.读取文件得到数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //从端口读取数据
//        SingleOutputStreamOperator<WaterSensor> waterSensorDataStreamSource = env.socketTextStream("localhost", 9999)
//                .map(new MapFunction<String, WaterSensor>() {
//                    @Override
//                    public WaterSensor map(String value) throws Exception {
//                        String[] split = value.split(",");
//                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
//                    }
//                });

        //3.将流转换为动态表
        Table table = tableEnvironment.fromDataStream(waterSensorDataStreamSource);

        //不注册直接使用
//        table.groupBy($("id"))
//                .flatAggregate(call(MyUDATF.class, $("vc")).as("value", "top"))
//                .select($("id"), $("value"), $("top"))
//                .execute()
//                .print();

        //注册自定义函数
        tableEnvironment.createTemporarySystemFunction("MyUDATF", MyUDATF.class);

        //TableAPI
        table.groupBy($("id"))
                //value是聚合得到的值，top是排名
                .flatAggregate(call("MyUDATF",$("vc")).as("value", "top"))
                .select($("id"), $("value"), $("top"))
                .execute()
                .print();

    }

    /**
     * Accumulator for Top2.
     */
    public static class Top2Accum {
        public Integer first;
        public Integer second;
    }

    //同过自定义UDTF函数来实现Top2的需求
    public static class MyUDATF extends TableAggregateFunction<Tuple2<Integer,Integer>, Top2Accum> {

        //初始化累加器
        @Override
        public Top2Accum createAccumulator() {
            Top2Accum acc = new Top2Accum();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        //执行累加计算
        public void accumulate(Top2Accum acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        //将结果发送出去
        public void emitValue(Top2Accum acc, Collector<Tuple2<Integer,Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }

            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}
