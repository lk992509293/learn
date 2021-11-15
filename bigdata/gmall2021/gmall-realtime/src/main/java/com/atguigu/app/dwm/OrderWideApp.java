package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.AsyncDimFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/*
测试开启APP顺序
OrderWideApp -> BaseDbApp -> FlinkCDCApp
 */

//TODO 需求：实现订单表和订单明细表的关联
//需求分析：关联订单表和订单明细表相当于mysql中对两个表做join，当id相同，需要提前定义好javabean对应订单表和订单明细表的列名，flink中的join
//订单表和订单明细表关联之后，好需要关联维度信息
// 分为按照滚动窗口，滑动窗口和interval的join，这个需求采用intervaljoin
public class OrderWideApp {
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

        //TODO 3.从kafka读取订单数据和订单明细数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        DataStreamSource<String> orderInfoDStream = env.addSource(MyKafkaUtil.getKafakaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailDStream = env.addSource(MyKafkaUtil.getKafakaConsumer(orderDetailSourceTopic, groupId));

        //打印查看从kafka获取到的数据格式
        //orderInfoDStream.print("order>>>>>");
//      order>>>>> {"delivery_address":"第8大街第5号楼7单元763门","consignee":"伍良海","create_time":"2021-09-22 22:15:06","order_comment":"描述695236",
//      "expire_time":"2021-09-22 22:30:06","original_total_amount":18785.00,"coupon_reduce_amount":0.00,"order_status":"1001","out_trade_no":"438396375156857",
//      "total_amount":18792.00,"user_id":62,"img_url":"http://img.gmall.com/219734.jpg","province_id":24,"feight_fee":7.00,"consignee_tel":"13605353970",
//      "trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米等4件商品","id":26965,"activity_reduce_amount":0.00}
//        orderDetailDStream.print("detail>>>>");

        //TODO 4.把订单数据和订单明细数据转换为javaBean格式
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoDStream.map(line -> {
            //先将数据格式转换为json格式，然后把json转换为JavaBean
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

            //获取创建时间
            String create_time = orderInfo.getCreate_time();

            //按照空格切分日期和时间
            String[] dateTimeArr = create_time.split(" ");

            //设置日期
            orderInfo.setCreate_date(dateTimeArr[0]);
            //设置小时
            orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

            //转换时间格式
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(dateFormat.parse(create_time).getTime());
            return orderInfo;
        });

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailDStream.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
            return orderDetail;
        });

        //TODO 5.提取事件时间生成watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        //orderInfoWithWMDS.print("orderInfoWithWMDS：");

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        //orderDetailWithWMDS.print("orderDetailWithWMDS：");

        //TODO 6.订单流与订单明细流双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //打印测试一下
        //orderWideDS.print("OrderWide");

        //TODO 7.关联维度信息，所以程序在map
        // 中需要去访问数据库中的维度表，此时与数据库的交互占据了函数大量的时间，导致运行效率低下，当流中的数据不能及时处理时，会产生层层反馈，产生反压现象，
        // 因此考虑使用异步交互，使map可以并发地处理多个请求和接收多个响应。这样，函数在等待的时间可以发送其他请求和接收其他响应。至少等待的时间可以被多个请求摊分。
        // 大多数情况下，异步交互可以大幅度提高流处理的吞吐量。
//        orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//            }
//
//            @Override
//            public OrderWide map(OrderWide value) throws Exception {
//                Long user_id = value.getUser_id();
//                Long province_id = value.getProvince_id();
//
//                return null;
//            }
//        });
        //TODO 7.1关联用户信息维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        //获取用户的性别
                        String gender = dimInfo.getString("GENDER");
                        orderWide.setUser_gender(gender);

                        //获取用户的出生日期
                        String birthday = dimInfo.getString("BIRTHDAY");
                        System.out.println("birthday = " + birthday);

                        //根据出生日期转换出用户的年龄
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long millis = System.currentTimeMillis();
                        long millisBirthday = sdf.parse(birthday).getTime();
                        long ts = millis - millisBirthday;

                        long age = ts / (1000L * 60 * 60 * 24 * 365);
                        orderWide.setUser_age((int) age);
                    }
                }, 60,
                TimeUnit.SECONDS);

        //打印订单表关联用户表的数据
        //orderWideWithUserDS.print("order_user>>>>>>>>>");

        //TODO 7.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithAreaDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                },
                60,
                TimeUnit.SECONDS);

        //orderWideWithAreaDS.print("order_Area>>>>>>>>>");

        //TODO 7.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS  = AsyncDataStream.unorderedWait(orderWideWithAreaDS,
                new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                },
                60, TimeUnit.SECONDS);

        //orderWideWithSpuDS.print("order_spu>>>>>>>>>>");

        //TODO 7.4关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //orderWideWithTmDS.print("order_Tm>>>>>>>>>>");

        //TODO 7.5关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("order_3DS>>>>>>>>");

        //TODO 8.将关联了所有维度的数据写入kafka
        DataStreamSink<OrderWide> orderWideSinkKafka =
                orderWideWithCategory3DS.addSink(MyKafkaUtil.getKafakaProducer(new KafkaSerializationSchema<OrderWide>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(OrderWide element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(orderWideSinkTopic,
                                JSON.toJSONBytes(element));
                    }
                }));


        //TODO 9.开启执行
        env.execute();
    }
}
