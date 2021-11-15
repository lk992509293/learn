package com.atguigu.cep;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.xml.parsers.SAXParser;
import java.time.Duration;
import java.util.List;
import java.util.Map;

//需求：统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
public class Flink02_CEP_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/OrderLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderEventStream =
                dataStreamSource.map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])
                        );
                    }
                });

        //设置watermark
        KeyedStream<OrderEvent, Long> orderEventKeyedStream = orderEventStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {

                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000;
                    }
                })
        ).keyBy(OrderEvent::getOrderId);

        //CEP
        //需求：统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
        //定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start")
                //创建订单
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                //松散连续模式
                .next("end")
                //.followedBy("end")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));
        
        //将数据流应用于模式
        PatternStream<OrderEvent> patternCEP = CEP.pattern(orderEventKeyedStream, pattern);
        
        //获取匹配结果,将不符合匹配规则则输出到侧输出流
        SingleOutputStreamOperator<String> result = patternCEP.select(
                new OutputTag<String>("side-output") {
                },
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString();
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        );

        //获取输出结果
        result.print();
        result.getSideOutput(new OutputTag<String>("side-output"){}).print("超时登录");

        env.execute();

    }
}
