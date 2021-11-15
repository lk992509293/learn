package com.atguigu.actual_combat;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink06_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据
        DataStreamSource<String> receiptStreamSource = env.readTextFile("input/ReceiptLog.csv");
        DataStreamSource<String> orderStreamSource = env.readTextFile("input/OrderLog.csv");

        //需求分析：匹配来自两条流的交易订单
        //先将两条流都转换为javaBean
        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderStreamSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String line) throws Exception {
                String[] split = line.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> txEvent = receiptStreamSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });
        
        //将两条流连接到一起
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventStream.connect(txEvent);

        //当两个交易数据的交易码相同时，说明是一个有效的交易订单
        //把交易码相同的数据聚集到一起
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = connect.keyBy("txId", "txId");

        //实时对账两条流的数据
        connectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            //创建Map集合用来缓存OrderEvent数据
            HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();

            //创建Map集合用来缓存TxEvent数据
            HashMap<String, TxEvent> txEventHashMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if (txEventHashMap.containsKey(value.getTxId())) {
                    //有能够关联上的交易码
                    out.collect("订单:" + value.getOrderId() + "对账成功");
                    //从缓存中删除该订单信息
                    txEventHashMap.remove(value.getTxId());
                } else {
                    //如果没有能够关联上的数据，就把当前订单的校验id放入缓存中
                    orderEventHashMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if (orderEventHashMap.containsKey(value.getTxId())) {
                    //有能够关联上的交易码
                    out.collect("订单:" + orderEventHashMap.get(value.getTxId()).getOrderId() + "对账成功");
                    //从缓存中删除该订单信息
                    orderEventHashMap.remove(value.getTxId());
                } else {
                    //如果没有能够关联上的数据，就把当前订单的校验id放入缓存中
                    txEventHashMap.put(value.getTxId(), value);
                }
            }
        }).print();

        //3.开启执行任务
        env.execute();
    }
}
