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
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink02_CEP_FloopMode {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/sensor.txt");

        //3.转换数据结构为javabean
        SingleOutputStreamOperator<WaterSensor> waterSensorStream  = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
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

        //4.定义watermark和乱序程度
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

        //5.按照id分组
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorOperator.keyBy("id");

        //CEP
        //TODO 1.定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                //TODO 1.1 使用量词 出现两次
                //.times(2);
                //TODO 1.1 使用量词 [2,4]   2次,3次或4次
                //.times(2, 4);
                //TODO 1.1表示大于等于1次
                //.oneOrMore();
                //TODO 1.1多次及多次以上,下式表示循环两次及两次以上
                .timesOrMore(2);

        //TODO 2.在流上应用
        PatternStream<WaterSensor> waterSensorPS  = CEP.pattern(keyedStream, pattern);

        //TODO 3.获取匹配到的结果
        waterSensorPS.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return  pattern.toString();
            }
        }).print();

        //6.开启执行
        env.execute();
    }
}
