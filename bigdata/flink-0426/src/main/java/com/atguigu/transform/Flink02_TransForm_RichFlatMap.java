package com.atguigu.transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_TransForm_RichFlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/1.txt");

        //3.转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Integer>> oneToTupleDstream = dataStreamSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("开始执行。。。。");
            }

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = line.split(" ");
                for (String str : split) {
                    out.collect(Tuple2.of(str, 1));
                }
            }

            @Override
            public void close() throws Exception {
                System.out.println("任务关闭。。。。");
            }
        });

        //4.输出并打印
        oneToTupleDstream.print();

        //5.开启执行任务
        env.execute();
    }
}
