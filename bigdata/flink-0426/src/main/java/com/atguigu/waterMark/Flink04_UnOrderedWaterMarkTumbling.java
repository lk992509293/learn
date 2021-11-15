package com.atguigu.waterMark;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


//基于事件时间的滚动窗口乱序watermark代码演示
public class Flink04_UnOrderedWaterMarkTumbling {
    public static void main(String[] args) throws Exception {
        //获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //将读取过来的数据转换为watersenor标准类
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] split = line.split(",");
                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
            }
        });

        //TODO 分配watermark并指定事件时间,设置乱序时间为两秒
        SingleOutputStreamOperator<WaterSensor> waterMarkAndEventTimestamp = waterSensorStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                //分配watermark
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                //分配事件时间戳
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                })
        );

        //将数据按照相同id聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterMarkAndEventTimestamp.keyBy("id");

        //基于事件时间开窗
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        })
                .print();
        keyedStream.sum("vc").print();

        env.execute();
    }
}
