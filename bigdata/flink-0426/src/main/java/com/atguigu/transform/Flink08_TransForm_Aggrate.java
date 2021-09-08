package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink08_TransForm_Aggrate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<WaterSensor> sensorList = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42),
                new WaterSensor("ws_001", 1577844025L, 46),
                new WaterSensor("ws_002", 1577844030L, 43),
                new WaterSensor("ws_003", 1577844035L, 42));

        //从集合中读取数据
        DataStreamSource<WaterSensor> dataStream = env.fromCollection(sensorList);

        KeyedStream<WaterSensor, String> keyedStream = dataStream.keyBy(WaterSensor::getId);

        //keyedStream.max("vc").print("max");

        //如果key相同，first参数设置为true则优先使用最早出现的值，否则使用最新值
        keyedStream.maxBy("vc", false).print("maxBy");

        env.execute();
    }
}
