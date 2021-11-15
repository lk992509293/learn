package com.atguigu.timer;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//需求：
//	监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
public class Flink01_Test_Timer {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(" ");
                return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        })
        );

        //3.按照传感器ID分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        //4.使用ProcessFunction实现5秒种水位不下降，则报警，且将报警信息输出到侧输出流
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            //用来记录上一次水位高度
            private Integer lastVc = Integer.MIN_VALUE;

            //用来记录定时器时间
            private Long timerTs = Long.MIN_VALUE;


            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {


                //判断当前水位线是否高于上次水位线
                if (value.getVc() > lastVc) {
                    //判断定时器是否重置，是否为第一条数据
                    if (timerTs == Long.MIN_VALUE) {
                        System.out.println("注册定时器。。。");
                        //注册5秒钟之后的定时器
                        System.out.println(ctx.timestamp());

                        timerTs = ctx.timestamp() + 5000L;
                        ctx.timerService().registerEventTimeTimer(timerTs);
                    }
                } else {
                    //如果水位线没有上升则删除定时器
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                    System.out.println("删除定时器。。。");
                    //将定时器的时间重置
                    timerTs = Long.MIN_VALUE;
                }
                //最后更新最新的水位线
                lastVc = value.getVc();
                System.out.println(lastVc);
                out.collect(value);
            }

            //定时器执行
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("sideOut") {
                           },
                        ctx.getCurrentKey() + "报警！！！！！！");
                //重置定时器时间
                timerTs = Long.MIN_VALUE;
            }
        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("sideOut") {
        }).print("报警信息");


        env.execute();
    }

}
