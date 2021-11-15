package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:9092"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //3.读取Kafka主题数据  dwd_payment_info  dwm_order_wide
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        
        DataStreamSource<String> orderWideKafkaDS  = env.addSource(MyKafkaUtil.getKafakaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentKafkaDS  = env.addSource(MyKafkaUtil.getKafakaConsumer(paymentInfoSourceTopic, groupId));

        //4.将数据转换为javaBean
        SingleOutputStreamOperator<OrderWide> orderWideDS =
                orderWideKafkaDS.map(data -> JSON.parseObject(data, OrderWide.class));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS =
                paymentKafkaDS.map(data -> JSON.parseObject(data, PaymentInfo.class));

        //5.设置watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWMDS = orderWideDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        //转换时间格式
                        long ts = 0L;
                        String create_time = element.getCreate_time();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            ts = sdf.parse(create_time).getTime();
                            return ts;
                        } catch (ParseException e) {
                            e.printStackTrace();
//                            throw new RuntimeException("时间格式错误！！！");
                            return ts;
                        }
                    }
                }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWMDS = paymentInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        //转换时间格式
                        long ts = 0L;
                        String create_time = element.getCreate_time();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            ts = sdf.parse(create_time).getTime();
                            return ts;
                        } catch (ParseException e) {
                            e.printStackTrace();
//                            throw new RuntimeException("时间格式错误！！！");
                            return ts;
                        }
                    }
                }));

        //6.采用双流join将订单表和支付表合并
        SingleOutputStreamOperator<PaymentWide> paymentWideDS  = orderWideWithWMDS.keyBy(OrderWide::getOrder_id)
                .intervalJoin(paymentInfoWithWMDS.keyBy(PaymentInfo::getOrder_id))
                .between(Time.seconds(-5), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide orderWide, PaymentInfo paymentInfo,
                                               Context ctx,
                                               Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //打印测试
        paymentWideDS.print("paymentWideDS>>>>>>>>>");

        //7.将数据写入kafka
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafakaProducer(paymentWideSinkTopic));

        //8.开启任务执行
        env.execute();
    }
}
