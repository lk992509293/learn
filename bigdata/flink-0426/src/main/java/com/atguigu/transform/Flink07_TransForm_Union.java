package com.atguigu.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_TransForm_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> dataStreamSource2 = env.fromElements(6, 7, 8, 9, 10);
        DataStreamSource<Integer> dataStreamSource3 = env.fromElements(11, 12, 13, 14, 15);

        //对三个流进行union操作
        DataStream<Integer> union = dataStreamSource1.union(dataStreamSource2, dataStreamSource3);

        SingleOutputStreamOperator<Integer> mapAndUnion = union.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });

        mapAndUnion.print();

        env.execute();
    }
}
