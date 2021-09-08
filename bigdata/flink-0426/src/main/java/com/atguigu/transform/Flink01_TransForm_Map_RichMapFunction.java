package com.atguigu.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Map_RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);

        SingleOutputStreamOperator<Integer> res = dataStreamSource.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public RuntimeContext getRuntimeContext() {
                System.out.println("RuntimeContext");
                return super.getRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open....执行一次");
            }

            @Override
            public void close() throws Exception {
                System.out.println("close....执行一次");
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return (int) Math.sqrt(value);
            }
        });

        System.out.println(res.getExecutionEnvironment());
        System.out.println(res.getName());
        System.out.println(res.getParallelism());
        System.out.println(res.getExecutionConfig());

        res.print();

        env.execute();
    }
}
