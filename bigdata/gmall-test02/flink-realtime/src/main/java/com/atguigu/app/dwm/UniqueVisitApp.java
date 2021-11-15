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

import java.io.IOException;
import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //从kafka读取数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        //转换数据格式为json
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);

        //按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //过滤掉重复访问的用户
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //声明状态
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(2))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lunch", String.class);
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws IOException {
                //获取上页id
                String lastPage = value.getJSONObject("page").getString("last_page_id");

                //如果上页id为null
                if (lastPage == null) {
                    //获取状态
                    String date = valueState.value();

                    //将数据的时间戳转换为日期字符串
                    String curDate = sdf.format(value.getString("ts"));

                    //判断状态是否为null，同时判断数据状态与当前数据日期是否相同
                    if (date == null || !date.equals(curDate)) {
                        //更新状态
                        valueState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //将数据写入kafka
        filterDS.print();
        //将数据格式转换为string
        filterDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        env.execute();
    }
}
