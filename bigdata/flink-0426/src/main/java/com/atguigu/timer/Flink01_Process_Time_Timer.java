package com.atguigu.timer;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//基于处理时间的定时器
public class Flink01_Process_Time_Timer {
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

        //4.将相同id的数据聚和到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //TODO 5.在process方法中注册并使用基于处理时间的定时器
        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.注册一个基于处理时间的定时器
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 200);
//                System.out.println("注册定时器："+ctx.timerService().currentProcessingTime()/1000+ctx.getCurrentKey());
            }

            /**
             * 定时器触发之后调用此方法
             *
             * @param timestamp
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 200);
                //out.collect("定时器被触发要起床了" + ctx.timerService().currentProcessingTime() / 1000 + ctx.getCurrentKey());
                System.out.println("生成WaterMark" + ctx.timerService().currentProcessingTime());
            }
        });

        process.print();

        env.execute();
    }
}
