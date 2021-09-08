package com.atguigu.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_TransForm_Filter {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.直接读取元素数据
        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        //3.过滤掉奇数
        SingleOutputStreamOperator<Integer> streamOperator = dataStreamSource.filter(t -> t % 2 != 1);

        //4.打印输出
        streamOperator.print();

        //5.开启任务执行
        env.execute();
    }
}
