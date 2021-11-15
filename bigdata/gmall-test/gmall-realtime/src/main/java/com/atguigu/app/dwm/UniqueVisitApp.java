package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.从kafka的dwd层读取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:9092"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka的dwd_page_log主题读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";

        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));
        
        //TODO 4.将获取的数据格式转换为json格式，如果不能转为json格式的数据要过滤掉
        SingleOutputStreamOperator<JSONObject> jsonDS = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = null;
                try {
                    jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        });

//        jsonDS.print("page>>>>");

        //TODO 5.我们计算日活，要针对用户id进行去重
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 6.对分组后的数据过滤，过滤掉不是今天第一次访问的数据，用状态记录第一次访问的时间
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //声明状态
            ValueState<String> valueState;

            //声明时间戳属性
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                //配置状态有效期TTL
                StateTtlConfig ttlConfig = StateTtlConfig
                        //设置状态有效期是一天
                        .newBuilder(Time.days(1))
                        //配置状态的更新策略
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                //获取状态描述器
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);

                //创建时间戳转换器
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //1.获取上一跳的页面id
                String lastPageId = value.getJSONObject("common").getString("last_page_id");

                //2.判断上一跳的页面id是否为null，如果不为null，则肯定不是首次登录，直接丢弃
                if (lastPageId == null || lastPageId.length() <= 0) {
                    //3.从状态中获取该mid登录的时间，如果获取到登陆时间，则不是首次登录，荣誉感以没有获取到，则为首次登录，并更新状态
                    String state = valueState.value();
                    if (state == null) {
                        valueState.update(sdf.format(value.getLong("ts")));
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        filterDS.print("UV>>>>");

        //TODO 7.将过滤后的日活数据写入kafka
        filterDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 开启任务执行
        env.execute();

    }
}
