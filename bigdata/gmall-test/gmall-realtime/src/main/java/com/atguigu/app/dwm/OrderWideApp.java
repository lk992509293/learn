package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.func.AsyncDimFunction;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从kafka读取事实数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId));

        //TODO 3.将数据格式转为javaBean格式
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = (SingleOutputStreamOperator<OrderInfo>) orderInfoKafkaDS.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                //将数据格式转换为javabean格式
                OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);

                //提取创建时间
                String create_time = orderInfo.getCreate_time();

                //将创建时间转换为数据流的事件时间戳
                String[] time = create_time.split(" ");

                //设置日期
                orderInfo.setCreate_date(time[0]);

                //设置时间（哪个小时）
                orderInfo.setCreate_hour(time[1].split(":")[0]);

                //转换时间格式
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                //设置创建时间戳
                orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                return orderInfo;
            }
        });

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailKafkaDS.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String value) throws Exception {
                //将字符串数据转为json后直接转换为javaBean格式
                OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);

                //转换时间格式
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                //将创建时间转为创建时间戳，作为事件时间
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        //TODO 4.设置watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //TODO 5.将订单表和订单表使用双流join连接在一起
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo,
                                               OrderDetail orderDetail,
                                               Context ctx,
                                               Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //TODO 6.关联维度信息
        //6.1关联用户维度表
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDS = AsyncDataStream.unorderedWait(orderWideDS, new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return input.getUser_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                //获取性别
                String gender = dimInfo.getString("GENDER");
                orderWide.setUser_gender(gender);

                //补充用户年龄度信息
                String birthday = dimInfo.getString("BIRTHDAY");

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                //获取当前系统时间
                long ts = System.currentTimeMillis() - sdf.parse(birthday).getTime();

                //将时间戳差转换为年龄
                long age = ts / (1000L * 60 * 60 * 24 * 365);

                orderWide.setUser_age((int) age);
            }
        }, 60, TimeUnit.SECONDS);

        //6.2关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserInfoDS, new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                //补充省份维度信息
                orderWide.setProvince_name(dimInfo.getString("Nmae"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);

        //6.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS, new AsyncDimFunction<OrderWide>("") {
            @Override
            public String getKey(OrderWide input) {
                return input.getSku_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                orderWide.setTm_id(dimInfo.getLong("TM_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        //6.4关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS, new AsyncDimFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return input.getSpu_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //6.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //6.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 7.将维度表信息的订单表写回到kafka的DWM层
        orderWideWithCategory3DS.map(JSON::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(orderWideSinkTopic));

        //TODO 开启任务执行
        env.execute();
    }
}
