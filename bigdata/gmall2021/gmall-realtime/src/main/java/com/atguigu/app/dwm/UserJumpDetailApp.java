package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
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
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建状态后端并设置检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //3.从kafka的dwd_page_log主题读取数据
        String groupId = "user_jump_detail_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtil.getKafakaConsumer(sourceTopic, groupId));

        //4.将数据格式转换为json格式
        SingleOutputStreamOperator<JSONObject> jsonDStream = kafkaDStream.map(JSONObject::parseObject);

        //5.设置watermark和乱序时间
        SingleOutputStreamOperator<JSONObject> jsonWaterDStream = jsonDStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //6.按照设备id分组，针对单个设备求跳出率
        KeyedStream<JSONObject, String> keyedStream = jsonWaterDStream.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //7.开启CEP，筛选跳出行为
        //筛选规则，用户首次进入页面随后就退出该页面记为一次跳出，那么只需要匹配到上一次访问的页面id是null，并且下一条数据的上一次访问页面id也是null
        // 那么“开始”事件就是一次跳出行为
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).next("end")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10));

        //8.将cep作用于流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //9.提取匹配上的和超时时间，并且把超时事件写入侧输出流
        SingleOutputStreamOperator<String> selectDStream =
                patternStream.select(new OutputTag<String>(
                                             "timeout") {
                                     },
                        new PatternTimeoutFunction<JSONObject, String>() {
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

        //10.拼接两个流并写入kafka
        DataStream<String> unionDStream =
                selectDStream.union(selectDStream.getSideOutput(new OutputTag<String>("timeout") {
                }));
        //打印
        unionDStream.print("uj>>>>>>>>>");
        //写入kafka
        unionDStream.addSink(MyKafkaUtil.getKafakaProducer(sinkTopic));

        //11.启动任务
        env.execute();
    }
}
