package com.atguigu.timer;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

//基于处理时间的定时器
public class Flink01_Event_Time_Timer {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop105", 9999);

        //3.将端口读过来数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //设置watermark
        SingleOutputStreamOperator<WaterSensor> waterSensorOperator = waterSensorStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        //分配watermark
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //分配watermark基于事件时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );

        //按照id分组聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorOperator.keyBy("id");

        //在process方法中注册一个时间定时器
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.注册一个定时器
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000);
                System.out.println("注册定时器：" + ctx.timestamp() / 1000 + ctx.getCurrentKey());
            }

            /**
             * @Author:lk
             * @Description:TODO 触发定时器后调用此方法
             * @DateTime:2021/9/12 18:45
             * @Params:* @param args
             * @Return:void
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("定时器被触发要起床了" + ctx.timestamp() + ctx.getCurrentKey());
            }
        }).print();

        env.execute();
    }
}
