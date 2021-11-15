package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

//数据流: web/app -> nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)
//程  序: Mock   -> Nginx -> Logger -> Kafka(ZK) -> BaseLogApp -> Kafka -> UniqueVisitApp -> Kafka
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置状态后端和checkpoint
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //数据主题
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        //2.从kafka dwd_page_log主题读取数据
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getKafakaConsumer(sourceTopic, groupId));

        //3.将从kafka读入的数据流转换为json对象，方便进行数据处理
        SingleOutputStreamOperator<JSONObject> jsonDStream = dataStreamSource.map(JSON::parseObject);
        //打印数据，查看数据格式
        jsonDStream.print();

        //4.将转换后json数据流按照设备id分组
        KeyedStream<JSONObject, String> midOfKeyedStream = jsonDStream.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //5.对数据在24小时的范围内过滤去重
        //需求分析：
        //5.1识别出第一次打开页面的用户，也就是说上一次的页面id为null
        SingleOutputStreamOperator<JSONObject> filterDS = midOfKeyedStream.filter(new RichFilterFunction<JSONObject>() {
            //声明状态
            private ValueState<String> dateState;

            //声明时间信息
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建时间格式
                sdf = new SimpleDateFormat("yyyy-MM-dd");

                //获取当前事件上下文状态
                ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("date-state", String.class);

                //设置状态的更新策略和注销策略(当再次访问时会更新状态时间，24小时未访问会清除当前状态)
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        //设置更新策略
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                descriptor.enableTimeToLive(ttlConfig);
                dateState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //1.获取上一跳页面ID
                String lastPage = value.getJSONObject("page").getString("last_page_id");

                //2.判断上一跳页面ID是否为Null
                if (lastPage == null) {
                    //lastPage为null则说明是第一次浏览该页面

                    //获取状态数据
                    String date = dateState.value();

                    //将时间戳转换为日期字符作为当前的事件时间
                    String curDate = sdf.format(value.getLong("ts"));

                    //5.2判断状态数据是否为Null同时判断状态数据与当前数据的日期是否相同
                    //当满足在状态中没有保存的数据或者不是状态日期的数据会被保留，就是首次访问的数据
                    if (date == null || !date.equals(curDate)) {
                        //更新状态
                        dateState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //6.将数据写入到kafka
        filterDS.print();
        filterDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafakaProducer(sinkTopic));

        //7.开启执行任务
        env.execute();
    }
}
