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
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink08_WaterMark_Tumbling_AllowedLateness_SideOut {
    public static void main(String[] args) throws Exception {
        //1.获取流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //将数据转换为Javabean
        SingleOutputStreamOperator<WaterSensor> waterMap = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                //按照“，”切割数据
                String[] split = line.split(",");

                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
            }
        });

        //设置watermark相关信息
        SingleOutputStreamOperator<WaterSensor> waterSensorStreamOperator = waterMap.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        //设置watermark
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //分配watermark时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );

        //按照id分组
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStreamOperator.keyBy("id");

        //开启一个基于事件的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //设置迟到时间
                .allowedLateness(Time.seconds(2))
                //将额外迟到的数据输出到侧输出流
                .sideOutputLateData(new OutputTag<WaterSensor>("side-out"){});

        SingleOutputStreamOperator<String> result = window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        });

        //输出主流
        result.print("主流");
        result.getSideOutput(new OutputTag<WaterSensor>("side-out"){}).print("迟到的数据");

        env.execute();
    }
}
