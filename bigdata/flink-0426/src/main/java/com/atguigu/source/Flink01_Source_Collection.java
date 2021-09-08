package com.atguigu.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //从集合中读取数据
//        List<WaterSensor> waterSensors = Arrays.asList(
//                new WaterSensor("ws_001", 1577844001L, 45),
//                new WaterSensor("ws_002", 1577844015L, 43),
//                new WaterSensor("ws_003", 1577844020L, 42));

        //从元素列表中读取数据
        List<String> asList = Arrays.asList("aaa", "bbb", "ccc");

        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从集合中读取数据
        DataStreamSource<List<String>> dataStreamSource = env.fromElements(asList);

        //3.打印输出结果
        dataStreamSource.print();

        //4.开启执行任务
        env.execute();
    }
}
