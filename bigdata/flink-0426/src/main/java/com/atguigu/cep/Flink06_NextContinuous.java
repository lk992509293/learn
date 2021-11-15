package com.atguigu.cep;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//组合模式
public class Flink06_NextContinuous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/sensor.txt");

        //转换数据结构为javabean
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

        //设置waterm
        SingleOutputStreamOperator<WaterSensor> waterSensorOperator = waterSensorStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        //分配watermark
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //分配事件时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );

        //按照id分组
        //KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorOperator.keyBy("id");

        //CEP
        //TODO 1.定义模式
        Pattern<WaterSensor, WaterSensor> parttern = Pattern.<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                //严格连续
                .next("end")
                //松散连续
                //.followedBy("end")
                //非确定的松散连续
                //.followedByAny("end")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                });

        //TODO 2.在流中应用模式
        PatternStream<WaterSensor> patternCEP = CEP.pattern(waterSensorOperator, parttern);

        //TODO 3.获取匹配到的结果
        patternCEP.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();
    }
}
