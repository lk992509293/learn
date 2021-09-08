package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class Flink10_TransForm_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<WaterSensor> sensorList = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 1),
                new WaterSensor("ws_001", 1577844015L, 1),
                new WaterSensor("ws_001", 1577844020L, 1),
                new WaterSensor("ws_002", 1577844025L, 1),
                new WaterSensor("ws_002", 1577844030L, 1),
                new WaterSensor("ws_002", 1577844035L, 1));

        //从集合中读取数据
        DataStreamSource<WaterSensor> dataStream = env.fromCollection(sensorList);

        KeyedStream<WaterSensor, String> keyedStream = dataStream.keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor> processStream = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor w, Context ctx, Collector<WaterSensor> out) throws Exception {
                out.collect(new WaterSensor(w.getId(), w.getTs(), w.getVc() * 2));
                System.out.println(ctx.getCurrentKey());
            }
        });

        processStream.print();

        env.execute();
    }
}
