package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.util.DateTimeUtil;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.phoenix.util.DateUtil;

import java.util.HashSet;

public class ProductStatsApp {
    public static void main(String[] args) {
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
        String groupId = "product_stats_app_210426";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> orderWideDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> cartInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> favorInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> refundInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(commentInfoSourceTopic, groupId));

        //TODO 4.将各表的数据结构转换为统一的javabean形式
        //4.1 pageViewDStream   页面详情点击数  曝光数
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                //将数据格式转换为json格式
                JSONObject jsonObject = JSON.parseObject(value);

                //获取时间戳
                long ts = jsonObject.getLong("ts");

                //获取当前页面ID以及item_type
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //获取曝光数
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        //3.2orderWideDStream  订单数(去重)，件数，订单总金额
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                //直接输入数据转换为json，再转换为订单宽表结构
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

                //去重
                HashSet<Long> orderIds = new HashSet<>();
                orderIds.add(orderWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .orderIdSet(orderIds)
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(DateTimeUtil.toTs(orderWide.getCreate_date()))
                        .build();
            }
        });

        //3.3 paymentWideDStream 支付的订单数，支付的总金额
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = paymentWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);

                //去重
                HashSet<Long> orderIds = new HashSet<>();
                orderIds.add(paymentWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(orderIds)
                        .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                        .build();
            }
        });

        //3.4 refundInfoDStream 退单数，退单金额
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refundOrderIdSet(orderIds)
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.6 cartInfoDStream  加购次数
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.7 commentInfoDStream 评价数，好评数
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
    }
}
