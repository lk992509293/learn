package com.atguigu.actual_combat;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink01_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流式数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文件中读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");

        //需求分析：网站独立访客数（UV）的统计，就是统计网站访客的人数，需要对访客去重
        //将读取到的数据转为javabean，并且收集属于访问类型行为的数据信息
        SingleOutputStreamOperator<Tuple2<String, Long>> uvDStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = line.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );

                //筛选出属于浏览网站的用户行为
                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(Tuple2.of("uv", userBehavior.getUserId()));
                }
            }
        });

        //对用户信息按照用户id分组
        KeyedStream<Tuple2<String, Long>, Long> keyedStream = uvDStream.keyBy(info -> info.f1);

        //对访问多次的用户去重
        keyedStream.process(new KeyedProcessFunction<Long, Tuple2<String, Long>, Integer>() {
            HashSet<Long> userIds = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                userIds.add(value.f1);
                out.collect(userIds.size());
            }
        }).print();

        //3.开启执行任务
        env.execute();
    }
}
