package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink09_TransForm_Reduce {
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

        SingleOutputStreamOperator<WaterSensor> reduceStream = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor w1, WaterSensor w2) throws Exception {
                return new WaterSensor(w1.getId(), w2.getTs(), w1.getVc() + w2.getVc());
            }
        });

        reduceStream.print();

        env.execute();
    }
}
