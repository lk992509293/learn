package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink06_Timer_Exec_With_State {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //3.装换数据为Javabean
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
            }
        });

        //4.将相同id的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //5.需求：监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //定义一个变量保存上一次的水位值
            ValueState<Integer> lastVc;

            //定义一个变量，用来存放定时器时间
            ValueState<Long> timer;

            //初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state",
                        Integer.class, 0));

                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class,
                        Long.MIN_VALUE));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.判断当前水位值是否大于上一次的水位
                if (value.getVc() > lastVc.value()) {
                    //注册定时器
                    if (timer.value() == Long.MIN_VALUE) {
                        //注册定时器
                        System.out.println("注册定时器。。。" + ctx.getCurrentKey());
                        timer.update(ctx.timerService().currentProcessingTime() + 5000);
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    }
                } else {
                    //当前水位值没有大于上一次的水位值
                    System.out.println("删除定时器" + ctx.getCurrentKey());
                    ctx.timerService().deleteProcessingTimeTimer(timer.value());

                    //重置定时器时间
                    timer.clear();
                }
                //将当前水位值保存到lastVc
                lastVc.update(value.getVc());
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                //定时器触发，定时器触发一次就重置一次定时器时间
                timer.clear();
                ctx.output(new OutputTag<String>("output") {
                }, "连续5s水位上升，报警！！！！！");
            }
        });

        //将报警信息在侧输出流打印
        result.getSideOutput(new OutputTag<String>("output"){}).print();

        //执行
        env.execute();
    }
}
