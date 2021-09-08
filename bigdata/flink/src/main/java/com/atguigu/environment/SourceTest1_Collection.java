package com.atguigu.environment;


import com.atguigu.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{
        //创建流式处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        //env.setParallelism(1);

        //source从集合中读取数据
        DataStreamSource<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_2", 1547718200L, 15.4),
                new SensorReading("sensor_3", 1547718201L, 5.8),
                new SensorReading("sensor_4", 1547718202L, 38.1)
                )
        );

        //打印
        source.print();

        //执行
        env.execute();
    }
}
