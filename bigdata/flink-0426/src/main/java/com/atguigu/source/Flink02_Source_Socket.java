package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Source_Socket {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从socket端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //3.输出并打印
        dataStreamSource.print();

        //4.开启执行任务
        env.execute();
    }
}
