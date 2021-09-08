package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //对原始数据进行转换
        SingleOutputStreamOperator<WaterSensor> flatMapDastream = dataStreamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String line, Collector<WaterSensor> out) throws Exception {
                String[] split = line.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        }).setParallelism(2);

        KeyedStream<WaterSensor, String> keyedStream = flatMapDastream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        flatMapDastream.print("原始数据：").setParallelism(2);
        keyedStream.print("keyby之后：").setParallelism(2);

        env.execute();
    }
}
