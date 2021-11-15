package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);


        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("ods_base_log", "ods_base_app"));

        //转换数据结构为json格式，如果不是json格式就过滤掉，将脏数据输出到侧输出流
        OutputTag<String> dirty = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonDS = dataStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirty, value);
                }
            }
        });

        //检验新老用户
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //使用状态保存用户的登陆时间
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //声明属性
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value" +
                        "-state",
                        String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");

                if ("1".equals(isNew)) {
                    //获取状态
                    String state = valueState.value();
                    if (state != null) {
                        //修改状态
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //更新状态
                        valueState.update("0");
                    }
                }
                return value;
            }
        });

        //TODO 分流
        OutputTag<String> startSide = new OutputTag<String>("start") {};
        OutputTag<String> displaySide = new OutputTag<String>("display") {};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    ctx.output(startSide, value.toJSONString());
                } else {
                    //将页面数据写入主流
                    out.collect(value.toJSONString());

                    //获取曝光日志
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        String pageId = value.getJSONObject("page").getString("page_id");

                        //遍历数组输出到侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面字段
                            display.put("page_id", pageId);

                            //输出到侧输出流
                            ctx.output(displaySide, display.toJSONString());
                        }
                    }
                }
            }
        });

        //获取启动日志
        DataStream<String> startSideOutput = pageDS.getSideOutput(startSide);
        DataStream<String> displaySideOutput = pageDS.getSideOutput(displaySide);

        //输出到kafka
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_page_log"));
        startSideOutput.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_start_log"));
        displaySideOutput.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_display_log"));

        env.execute();
    }
}
