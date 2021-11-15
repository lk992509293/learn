package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:9092"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka的pv、uv、跳转明细主题中获取数据
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageDs = env.addSource(MyKafkaUtil.getKafakaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafakaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafakaConsumer(userJumpDetailSourceTopic, groupId));

//        pageDs.print("pageDs:");
//        uvDS.print("uvDS:");
//        ujDS.print("ujDS:");

        //TODO 4.将三个流的数据全部转换为同一个javaBean的形式，先将String -> JSON
        //4.1获取页面流的javaBean的形式
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPageDS = pageDs.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                //获取common中的数据
                JSONObject common = jsonObject.getJSONObject("common");
                //获取进入页面的次数
                Long sv = jsonObject.getJSONObject("page").getString("last_page_id") == null ?
                        1L : 0L;
                //统计的开始时间和结束时间要去窗口中获取开窗时间和窗口关闭时间
                return new VisitorStats(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L, 1L, sv, 0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });

        //4.2获取用户独立访问数据流uv的javaBean的形式
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new VisitorStats("", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        1L, 0L, 0L, 0L, 0L,
                        jsonObject.getLong("ts"));
            }
        });

        //4.3获取跳出次数数据javaBean的形式
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new VisitorStats("", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L, 0L, 0L, 1L, 0L,
                        jsonObject.getLong("ts"));
            }
        });

        //TODO 5.union三个流
        DataStream<VisitorStats> unionDS = visitorStatsWithPageDS.union(visitorStatsWithUvDS, visitorStatsWithUjDS);

        //TODO 6.提取事件时间戳，生成watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 7.选取四个维度分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = unionDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(), value.getCh(), value.getVc(), value.getIs_new());
            }
        });
//        keyedStream.print("key>>>>");

        //TODO 8.开一个10s的事件时间窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 9.在窗口内聚合PV、UV、跳出次数、进入页面数(session_count)、连续访问时长
        SingleOutputStreamOperator<VisitorStats> resultDS = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());

                //System.out.println("value1 = " + value1);
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                //获取数据
                VisitorStats visitorStats = input.iterator().next();

                //获取窗口开始时间和结束时间
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String start = sdf.format(window.getStart());
                String end = sdf.format(window.getEnd());

                //将窗口的开始和关闭时间信息补充道javaBean
                visitorStats.setStt(start);
                visitorStats.setEdt(end);

                //输出结果
                System.out.println("visitorStats = " + visitorStats);
                out.collect(visitorStats);
            }
        });

        //打印数据
        resultDS.print("VisitorStats>>>>>>>>>");

        //TODO 将数据输出到ClickHouse
//        resultDS.addSink(ClickHouseUtil.getClickHouse("insert into visitor_stats values(?,?,?,?," +
//                "?,?,?,?,?,?,?,?)"));

        //TODO 10.开启任务执行
        env.execute();
    }
}
