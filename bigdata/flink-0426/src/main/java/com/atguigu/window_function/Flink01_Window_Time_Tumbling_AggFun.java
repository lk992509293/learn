package com.atguigu.window_function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink01_Window_Time_Tumbling_AggFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //3.将数据组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = wordToOneStream.keyBy(0);

        //开启一个基于时间的滑动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window =
                tuple2TupleKeyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

//        window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> createAccumulator() {
//                return Tuple2.of(null, 0);
//            }
//
//            @Override
//            public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
//                return Tuple2.of(value.f0, accumulator.f1 + value.f1);
//            }
//
//            @Override
//            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
//                return Tuple2.of(accumulator.f0, accumulator.f1);
//            }
//
//            @Override
//            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
//                return Tuple2.of(a.f0, a.f1 + b.f1);
//            }
//        }).print();

        window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                return value.f1 + accumulator;
            }

            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        }).print();

        env.execute();
    }
}
