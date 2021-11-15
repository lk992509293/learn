package com.atguigu.tableapi$sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink20_SQL_GroupWindow_Sliding_Hop {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 3.创建表
        tableEnvironment.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")");

        //TODO 开启滑动窗口
        tableEnvironment.executeSql("select\n" +
                " id,\n" +
                " sum(vc) vcSum, \n" +
                //设置滑动窗口的开始时间，窗口的大小为3秒，滑动步长为2秒
                " HOP_START(t,INTERVAL '2' second,INTERVAL '3' second) as startWindow, \n" +
                " HOP_END(t,INTERVAL '2' second,INTERVAL '3' second) as endWindow\n" +
                "from sensor \n" +
                "group by HOP(t,INTERVAL '2' second,INTERVAL '3' second), id")
                .print();
    }
}
