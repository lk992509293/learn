package com.atguigu.environment;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\1.txt");

        //打印输出
        dataStream.print();

        //执行任务
        env.execute();
    }
}
