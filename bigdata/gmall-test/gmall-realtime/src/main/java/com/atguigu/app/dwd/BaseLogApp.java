package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

//准备用户行为日志DWD层
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka的ods_base_log主题读取原始日志数据
        //定义生产者和消费者主题
        String groupId = "ods_dwd_base_log_app";
        String ods_source_topic = "ods_base_log";

        String dwd_page_topic_sink = "dwd_page_log";
        String dwd_start_topic_sink = "dwd_start_log";
        String dwd_display_topic_sink = "dwd_display_log";

        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ods_source_topic, groupId));

        //TODO 4.转换数据格式为json格式
        SingleOutputStreamOperator<JSONObject> jsonDS = dataStreamSource.map(JSON::parseObject);

        //TODO 5.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 6.对分组后的流确认新老用户
        SingleOutputStreamOperator<JSONObject> isNewDS =
                keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    //声明状态
                    private ValueState<String> valueState;

                    //声明登陆时间
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                                "is_new", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");

                        //如果当前是信用户则需要校验
                        if ("1".equals(isNew)) {
                            //去状态中获取状态
                            String state = valueState.value();
                            //获取登录时间戳
                            Long ts = value.getLong("ts");
                            if (state != null) {
                                //修复该条数据
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                //更新状态
                                valueState.update(sdf.format(ts));
                            }
                        }
                        out.collect(value);
                    }
                });

        //TODO 7.对数据分流，将page数据输出到主流，将启动日志和曝光日志输出到测输出流
        //7.1创建启动日志和曝光日志的侧输出流
        OutputTag<String> startSide = new OutputTag<String>("start"){};
        OutputTag<String> displaySide = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> pageDS = isNewDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //提取启动关键字
                String start = value.getString("start");
                //将启动日志输出到“start”侧输出流
                if (start != null && start.length() > 0) {
                    ctx.output(startSide, value.toJSONString());
                } else {
                    //页面数据输出到主流
                    out.collect(value.toJSONString());

                    //提取曝光日志标记
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光日志数组，将每一条数据都存储到曝光日志侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            //取出单条数据
                            JSONObject jsonObject = displays.getJSONObject(i);
                            //添加页面id
                            jsonObject.put("page", value.getJSONObject("page").getString("page_id"));
                            //输出到侧输出流
                            ctx.output(displaySide, jsonObject.toJSONString());
                        }
                    }
                }
            }
        });

        //打印
        pageDS.print("page>>>>");
//        pageDS.getSideOutput(startSide).print("start>>>>");
//        pageDS.getSideOutput(displaySide).print("display>>>>");

        //TODO 8.将三条流分别输出到对用的主题中
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(dwd_page_topic_sink));
        //获取启动日志侧输出流
        pageDS.getSideOutput(startSide).addSink(MyKafkaUtil.getFlinkKafkaProducer(dwd_start_topic_sink));
        //获取曝光日志侧输出流
        pageDS.getSideOutput(displaySide).addSink(MyKafkaUtil.getFlinkKafkaProducer(dwd_display_topic_sink));

        //TODO 开启执行任务
        env.execute();
    }
}
