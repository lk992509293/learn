package com.atguigu.UDF;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LambdaFunctions {
    public static void main(String[] args) throws Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(4);

        //读取数据
        DataStream<String> dataStream = env.fromElements("hello flink", "hello spark", "java flink");

        //通过Lambda表达式获取含有flink的字符串
        SingleOutputStreamOperator<String> filterStream = dataStream.filter(str -> str.contains("flink"));

        filterStream.print();

        env.execute();
    }
}
