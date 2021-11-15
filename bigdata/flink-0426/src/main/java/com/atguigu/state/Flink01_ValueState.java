package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_ValueState {
    //需求：检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //3.将数据类型转换为Javabean
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

        //4.按照id分组
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //5.从状态中获取前后两次数据，并计算二者差值
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //声明valuestate存储
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取当前运行状态
//                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //获取状态中上一次的值
                int lastVc = valueState.value() == null ? 0 : valueState.value();

                if (Math.abs(value.getVc() - lastVc) > 10) {
                    out.collect(value.getId() + "警报！！！！");
                }
                valueState.update(value.getVc());
            }
        })
                .print();

        env.execute();
    }
}
