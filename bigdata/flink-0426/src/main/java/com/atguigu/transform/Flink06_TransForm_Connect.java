package com.atguigu.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink06_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        DataStreamSource<String> dataStreamSource2 = env.fromElements("1", "2", "3", "4", "5");

        //将两个流连接
        ConnectedStreams<Integer, String> connect = dataStreamSource1.connect(dataStreamSource2);

        //转换数据
        SingleOutputStreamOperator<Object> coMapStream = connect.map(new CoMapFunction<Integer, String, Object>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                return value + 1;
            }

            @Override
            public String map2(String value) throws Exception {
                return 'a' + value;
            }
        });

        coMapStream.print();

        env.execute();
    }
}
