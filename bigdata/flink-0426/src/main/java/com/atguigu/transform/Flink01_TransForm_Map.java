package com.atguigu.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Map {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.直接读取元素中的值
        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);

        //3.通过map算子让每一个数字值+1
        SingleOutputStreamOperator<Integer> res = dataStreamSource.map(v -> v + 1);

        //4.打印输出结果
        res.print();

        //5.开启执行任务
        env.execute();
    }
}
