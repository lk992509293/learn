package com.atguigu.flink;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //按照空格切分，然后以key统计计算
        DataStreamSource<String> input = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\1.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountDataStream = input.flatMap(new WordCountSet.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        wordCountDataStream.print().setParallelism(8);


        //执行任务
        env.execute();
    }
}
