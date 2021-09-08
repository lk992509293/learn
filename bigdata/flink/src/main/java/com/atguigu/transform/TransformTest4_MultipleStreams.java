package com.atguigu.transform;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

//以36度为界，将输入流拆分为两个流
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(1);

        //读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\2.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0], new Long(strings[1]), new Double(strings[2]));
        });

        //将输入流以36度为界拆分
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTemperature() > 36) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");
        DataStream<SensorReading> all = splitStream.select("high", "low");

        //打印输出
//        high.print();
//        low.print();
        all.print();

        //2.合流connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        //连接
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = warningStream.connect(low);

        SingleOutputStreamOperator<Object> resultStream = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print();

        //3.联合多条流
        DataStream<SensorReading> union = high.union(low, all);
        union.print();

        //执行
        env.execute();
    }
}
