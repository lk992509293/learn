package com.atguigu.repartition;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


//轮询的方式重分区
public class Reblance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        dataStreamSource.print("原始数据");

        //rablance重分区
        //dataStreamSource.rebalance().print("轮询重分区");

        //rescale重分区
        dataStreamSource.rescale().print("rescale重分区");

        env.execute();
    }
}
