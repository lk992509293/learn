package com.atguigu.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.util.ClickHouseUtil;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka读取数据
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId));

        //TODO 4.将从kafka读取的数据转换为统一的javabean格式
        //将页面访问数，进入页面数，连续访问时长封装到javabean格式中
        SingleOutputStreamOperator<VisitorStats> pvStateDS = pvDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //将字符串转换为json格式
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");

                //进入次数通过页面来求，如果上页id是null，则说明是首次进入
                String last_page = JSON.parseObject("page").getString("last_page_id");
                long sv = last_page == null ? 1L : 0L;

                return new VisitorStats("", "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L, 1L, sv, 0L,
                        JSON.parseObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });

        //将访客指标uv转换为javabean格式
        SingleOutputStreamOperator<VisitorStats> uvStateDS = uvDS.map(new MapFunction<String, VisitorStats>() {
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
                        jsonObject.getLong("ts")
                );
            }
        });

        //将跳出指标uj转换为javabean
        SingleOutputStreamOperator<VisitorStats> ujStateDS = ujDS.map(new MapFunction<String, VisitorStats>() {
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
                        jsonObject.getLong("ts")
                );
            }
        });

        //TODO 5.将三个流使用union进行连接
        DataStream<VisitorStats> unionDS = pvStateDS.union(uvStateDS, ujStateDS);

        //TODO 6.获取watermark
        SingleOutputStreamOperator<VisitorStats> visitorWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 7.分组，开窗，聚合
        SingleOutputStreamOperator<VisitorStats> windowedStream = visitorWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getVc(),
                        value.getIs_new());
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());

                        return value1;
                    }
                }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String,
                        String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        //获取数据
                        VisitorStats visitorStats = input.iterator().next();

                        //读取窗口信息，补充开始和结束时间
                        long start = window.getStart();
                        long end = window.getEnd();

                        //转换时间格式
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String stt = sdf.format(start);
                        String edt = sdf.format(end);

                        visitorStats.setStt(stt);
                        visitorStats.setEdt(edt);

                        //输出数据
                        out.collect(visitorStats);
                    }
                });

        //TODO 8.将数据写到clickhouse
        windowedStream.addSink(ClickHouseUtil.getClickHouse("insert into visitor_stats_210426 " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute();
    }
}
