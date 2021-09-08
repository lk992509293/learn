package com.atguigu.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_TransForm_FlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文件读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/1.txt");

        //3.数据转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> oneToTupleStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = line.split(" ");
                for (String str : split) {
                    out.collect(new Tuple2<>(str, 1));
                }
            }
        });

        //4.输出打印
        oneToTupleStream.print();

        //5.开启执行任务
        env.execute();
    }
}
