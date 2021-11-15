package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.AsyncDimFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointInterval(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka读取dwd层数据获取数据流,7个主题的数据创建流
        String groupId = "product_stats_app_210426";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafakaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafakaConsumer(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafakaConsumer(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSource = MyKafkaUtil.getKafakaConsumer(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafakaConsumer(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafakaConsumer(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafakaConsumer(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSource);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 4.将json格式的数据流转换为统一的javabean数据对象
        //4.1根据读取到的页面数据流，获取点击数和曝光数，由于需要一次获取两部分数据，所以使用flatmap处理
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                //获取时间戳
                Long ts = jsonObject.getLong("ts");

                //获取当前页面的id以及item_type
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //获取曝光数据
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

        //4.2 求收藏次数
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            //获取时间戳
            Long ts = jsonObject.getLong("ts");

            return ProductStats.builder().
                    sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(ts)
                    .build();
        });

        //4.3 求订单总次数以及订单的金额
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {
            //获取Jason数据，直接将json格式转为javabean
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            //创建一个hashset对订单去重
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .orderIdSet(orderIds)
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        //4.4 paymentWideDStream 支付的订单数，支付的总金额
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = paymentWideDStream.map(line -> {

            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        //4.5 refundInfoDStream 退单数，退单金额
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

        //4.6 cartInfoDStream  加购次数
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //4.7 commentInfoDStream 评价数，好评数
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

        //TODO 5.将读取到所有数据流合并成一个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStatsWithFavorDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCartDS,
                productStatsWithCommentDS);

        //TODO 6.给流按照事件时间设置水位线
        SingleOutputStreamOperator<ProductStats> productStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 7.分组，开窗，聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWmDS.keyBy(ProductStats::getSku_id).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                                //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                                //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                                //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                                return stats1;
                            }
                        },
                        new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                                //获取数据
                                ProductStats productStats = input.iterator().next();

                                //补充订单数
                                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                                //补充窗口的开始和结束时间
                                productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                                //输出数据
                                out.collect(productStats);
                            }
                        });

        //TODO 8.关联维度补充数据
        //8.1关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS, new AsyncDimFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getId(ProductStats input) {
                return input.getSku_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException, Exception {
                productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                productStats.setTm_id(dimInfo.getLong("TM_ID"));
                productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        //8.2关联SPU维度信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS, new AsyncDimFunction<ProductStats>("") {
            @Override
            public String getId(ProductStats input) {
                return input.getSpu_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException, Exception {
                productStats.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //8.3 关联TM维度信息
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithSpuDS, new AsyncDimFunction<ProductStats>("") {
            @Override
            public String getId(ProductStats input) {
                return String.valueOf(input.getTm_id());
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException, Exception {
                productStats.setTm_name(dimInfo.getString("DIM_BASE_TRADEMARK"));
            }
        }, 60, TimeUnit.SECONDS);

        //8.4 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithTmDS, new AsyncDimFunction<ProductStats>("DIM_BASE_CATEGORY3") {
            @Override
            public String getId(ProductStats input) {
                return String.valueOf(input.getCategory3_id());
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException, Exception {
                productStats.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //打印
        productStatsWithCategory3DS.print("Product>>>>");
        //将数据输出到clickhouse
//        productStatsWithCartDS.addSink(ClickHouseUtil.getClickHouse("insert into product_stats " +
//                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.开启任务执行
        env.execute();
    }
}
