package com.atguigu.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

//计算有界流
public class WordCount_Bound_Test02 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //设置全局并行度
        env.setParallelism(2);

        //2.从文中读取数据
        //DataStreamSource<String> dataStream = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink-0426\\input\\word.txt");
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop105", 9999);

        //3.将上游发过来的数据拆分
        SingleOutputStreamOperator<String> wordStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    out.collect(word);
                }
            }
        });

        //4.转换数据格式，并分组聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).slotSharingGroup("group 1").setParallelism(1);

        //5.分组聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = wordToOneStream.keyBy(t -> t.f0).sum(1);

        //6.打印输出结果
        res.print();

        //7.开启任务
        env.execute();
    }
}
