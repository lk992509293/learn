package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流:web/app -> nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp   -> Kafka(DWD)
//程  序:mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境,并行度设置与Kafka主题的分区数一致

        //2.设置状态后端和checkpoint，生产环境必须设置，测试环境为了方便暂时不设置
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.消费Kafka ods_base_log 主题数据创建流
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getKafakaConsumer("ods_base_log", "ods_base_app"));

        //TODO 4.将数据转换为JsonObject格式
        //如果存在脏数据，就需要把脏数据输出到侧输出流，或者程序会出现异常挂掉
        OutputTag<String> dirty = new OutputTag<String>("dirty"){};

        //用到侧输出流必然使用process算子
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                //如果存在脏数据会出现异常，比如数据不是json格式，所以需要异常处理
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirty, value);
                }
            }
        });

        //获取脏数据并打印
        jsonObjDS.getSideOutput(dirty).print("dirty");

        //TODO 5.需求一：识别新老用户，新老用户的识别是根据页面日志中的“id_new”来判断的，id_new=1是新用户，id_new=0
        // 是老用户，前端埋点采集的数据中，如果用户卸载了app，就会导致老用户被识别为新用户，所以使用flink状态来保存用户信息，从状态中判断该用户（mid）是否出现过
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(info -> info.getJSONObject("common").getString("mid"));

        //TODO 5.1根据状态校验新老用户
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //保存状态
            private ValueState<String> valueState;

            //初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value" +
                        "-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //a.获取is_new标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //b.判断isNew当前的值
                if ("1".equals(isNew)) {
                    //从状态中去获取数据，如果状态已经保存了当前数据，则说明不是新用户
                    String state = valueState.value();
                    if (state != null) {
                        //修改标记
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //更新状态
                        valueState.update("0");
                    }
                }
                return value;
            }
        });

        //TODO 6.需求二：利用侧输出流实现数据拆分
        //TODO 需求分析：一共有三个数据，分别是页面日志（page） -> 主流，启动日志（start） -> 侧输出流，曝光日志（display） -> 侧输出流

        //创建启动日志的侧输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};

        //创建曝光日志的侧输出流
        OutputTag<String> displayOutputTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //获取启动日志数据
                String start = value.getString("start");
                if (start != null) {
                    //将启动日志写入侧输出流
                    ctx.output(startOutputTag, start);
                } else {
                    //将页面数据写入主流
                    out.collect(value.toJSONString());

                    //提取曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        String pageId = value.getJSONObject("page").getString("page_id");
                        //遍历曝光数据，将曝光日志写出到测输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            //在displaysJSONObject多插入一条页面信息
                            displaysJSONObject.put("page_id", pageId);
                            ctx.output(displayOutputTag, displaysJSONObject.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 7.将不同流的数据推送下游的Kafka的不同Topic中
        //7.获取启动日志侧输出流
        DataStream<String> startSideOutput = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displaySideOutput = pageDS.getSideOutput(displayOutputTag);

        //7.2打印
        pageDS.print("Page>>>>>>>>>");
        startSideOutput.print("Start>>>>>>>>>");
        displaySideOutput.print("Display>>>>>>>");

        //7.3将主流与侧输出流输出到kafka
        pageDS.addSink(MyKafkaUtil.getKafakaProducer("dwd_page_log"));
        startSideOutput.addSink(MyKafkaUtil.getKafakaProducer("dwd_start_log"));
        displaySideOutput.addSink(MyKafkaUtil.getKafakaProducer("dwd_display_log"));

        //TODO 8.启动任务
        env.execute();
    }
}
