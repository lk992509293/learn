package com.atguigu.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_TransForm_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        dataStreamSource.print("原始数据");
        dataStreamSource.shuffle().print("shuffle数据");

        env.execute();
    }
}
