package com.atguigu.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

//广播状态
public class Flink07_BroadcastOperatorState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.并行度设置为1
        env.setParallelism(1);

        //3.从端口读取数据
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("hadoop105", 9999);
        DataStreamSource<String> dataStreamSource2 = env.socketTextStream("hadoop106", 9999);

        //4.定义广播状态
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("state", String.class, String.class);

        BroadcastStream<String> broadcast = dataStreamSource1.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<String, String> connect = dataStreamSource2.connect(broadcast);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //获取状态中的值
                String s = broadcastState.get("switch");
                if ("1".equals(s)) {

                    out.collect("执行逻辑1。。。");
                } else if ("2".equals(s)) {

                    out.collect("执行逻辑2。。。");
                } else {

                    out.collect("执行其他逻辑");
                }

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                //提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                //将数据放入广播状态中
                broadcastState.put("switch", value);
            }
        }).print();

        env.execute();

    }
}
