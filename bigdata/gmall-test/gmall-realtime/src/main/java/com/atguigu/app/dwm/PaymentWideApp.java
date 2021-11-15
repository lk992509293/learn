package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:9092/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka读取订单宽表数据和支付明细数据
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> orderSourceDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentSourceDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId));

        //TODO 3.将流转换为javaBean
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderSourceDS.map(data -> JSON.parseObject(data, OrderWide.class));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentSourceDS.map(data -> JSON.parseObject(data, PaymentInfo.class));

        //TODO 4.生成watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWmDS = orderWideDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            return recordTimestamp;
                        }
                    }
                }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWmDS = paymentInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            return recordTimestamp;
                        }
                    }
                }));

        //TODO 5.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = orderWideWithWmDS.keyBy(OrderWide::getOrder_id)
                .intervalJoin(paymentInfoWithWmDS.keyBy(PaymentInfo::getOrder_id))
                .between(Time.seconds(-2), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide order, PaymentInfo payment, Context ctx,
                                               Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(payment, order));
                    }
                });

        //TODO 6.将支付款表数据写入kafka
        paymentWideDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(paymentWideSinkTopic));

        //TODO 7.开启任务执行
        env.execute();

    }
}
