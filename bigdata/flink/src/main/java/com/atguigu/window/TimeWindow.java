package com.atguigu.window;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\2.txt");

        SingleOutputStreamOperator<Tuple2<String, Double>> dataStream = inputStream.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String line) throws Exception {
                String[] fields = line.split(",");
                return new Tuple2<>(fields[0], new Double(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> resultStream = dataStream.keyBy(t -> t.f0)
                .countWindow(5)
                .sum(1);


        resultStream.print();

        env.execute();
    }
}
