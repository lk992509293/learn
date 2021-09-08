package com.atguigu.transform;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置最大并行度
        env.setParallelism(4);

        //从文件中读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\2.txt");

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");

            return new SensorReading(strings[0], new Long(strings[1]), new Double(strings[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());

        DataStream<Long> dataStream1 = env.fromElements(1L, 34L, 4L, 657L, 23L);
        KeyedStream<Long, Integer> keyedStream2 = dataStream1.keyBy(new KeySelector<Long, Integer>() {
            @Override
            public Integer getKey(Long value)  {
                return value.intValue() % 2;
            }
        });

        //KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合，取当前最大的温度值
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print("result");

        keyedStream1.print("key1");
        keyedStream2.sum(0).print("key2");
        env.execute();
    }
}
