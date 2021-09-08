package com.atguigu.transform;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_TransForm_RichFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        SingleOutputStreamOperator<Integer> res = dataStreamSource.filter(new RichFilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 != 1;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("开启任务。。。。");
            }

            @Override
            public void close() throws Exception {
                System.out.println("关闭任务。。。。");
            }
        });

        res.print();

        env.execute();
    }
}
