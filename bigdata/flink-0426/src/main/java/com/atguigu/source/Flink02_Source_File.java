package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中读取数据
        //DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink-0426\\input\\word.txt");

        //从目录中读取数据
        //DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink-0426\\input");

        //从HDFS中读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("hdfs://hadoop105:8020/flink/test");

        //3.输出并打印数据
        dataStreamSource.print();

        //4.开启执行任务
        env.execute();
    }
}
