package com.atguigu.cep;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//需求：用户2秒内连续两次及以上登录失败则判定为恶意登录。
public class Flink01_CEP_Project_Login {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/LoginLog.csv");

        SingleOutputStreamOperator<LoginEvent> loginEventStream= dataStreamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        //设置watermark,然后按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {

                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000;
                    }
                })).keyBy(LoginEvent::getUserId);

        //需求：用户2秒内连续两次及以上登录失败则判定为恶意登录。
        //CEP
        //定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                //两次及以上
                .timesOrMore(2)
                //连续
                .consecutive()
                //两秒内
                .within(Time.seconds(2));

        //将模式作用于流上
        PatternStream<LoginEvent> patternCEP = CEP.pattern(keyedStream, pattern);

        //获取匹配到的结果
        patternCEP.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();
    }
}
