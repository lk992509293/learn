package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(2000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);

        //获取watermark
        SingleOutputStreamOperator<JSONObject> jsonWMDs = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //分组
        KeyedStream<JSONObject, String> keyedDS = jsonWMDs.keyBy(data -> JSON.parseObject("common").getString("mid"));

        //使用CEP
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("end")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10));
        
        //将CEP作用于主流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedDS, pattern);

        //侧输出流输出超时事件
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };

        //提取事件
        SingleOutputStreamOperator<String> selectDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.get("begin").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                return pattern.get("begin").get(0).toJSONString();
            }
        });

        //获取侧输出流
        DataStream<String> sideOutput = selectDS.getSideOutput(outputTag);

        //拼接两个流，输出到kafka
        DataStream<String> unionDS = selectDS.union(sideOutput);

        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        env.execute();

    }
}
