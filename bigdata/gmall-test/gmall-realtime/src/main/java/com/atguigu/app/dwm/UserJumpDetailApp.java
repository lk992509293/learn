package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
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

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:9092/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        //TODO 4.转换数据格式为json
        SingleOutputStreamOperator<JSONObject> jsonDS = dataStreamSource.map(JSON::parseObject);

        //TODO 5.设置水印，获取watermark
        SingleOutputStreamOperator<JSONObject> jsonWithWMDS =
                jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));

        //TODO 6.对数据按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonWithWMDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 7.使用CEP状态编程
        //筛选规则，用户首次进入页面随后就退出该页面记为一次跳出，那么只需要匹配到上一次访问的页面id是null，并且下一条数据的上一次访问页面id也是null
        // 那么“开始”事件就是一次跳出行，如果超时了，也是一次跳出行为
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            //第一条数据
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return null == value.getJSONObject("page").getString("last_page_id");
            }
        }).next("next")
                //第二条数据
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return null == value.getJSONObject("page").getString("last_page_id");
                    }
                }).within(Time.seconds(10));

        //TODO 8.将CEP状态加载到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 9.提取匹配到数据输出到主流，将超时数据输出到侧输出流
        SingleOutputStreamOperator<JSONObject> selectDStream = patternStream.select(new OutputTag<JSONObject>("timeout") {
                                                                                    },
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    //参数是map的原因，其实map中的key就是CEP设置的不同阶段的名字
                    //map中的value是list的原因，在CEP模式中可以添加循环模式，如果是循环模式，一个可以就回对应多条数据
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        //直接从开始状态拿到数据，由于本流中都是单个模式，所以list中只有一条数据
                        return pattern.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("start").get(0);
                    }
                });

        //TODO 10.拼接两个流，然后把数据写入kafka
        SingleOutputStreamOperator<String> outPutTag = selectDStream.getSideOutput(new OutputTag<JSONObject>("timeout") {
        }).map(JSONAware::toJSONString);

        DataStream<String> unionDS = selectDStream.map(JSONAware::toJSONString).union(outPutTag);
        unionDS.print("uj>>>>");
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 11.开启任务执行
        env.execute();
    }
}
