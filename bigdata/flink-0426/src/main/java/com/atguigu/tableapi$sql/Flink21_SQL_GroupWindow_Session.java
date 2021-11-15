package com.atguigu.tableapi$sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink21_SQL_GroupWindow_Session {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 创建表
        tableEnv.executeSql("create table sensor(" +
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

        //TODO 开启一个会话窗口
        tableEnv.executeSql("select\n" +
                " id,\n" +
                " sum(vc) vcSum,\n" +
                //设置会话窗口开始时间和会话间隔为2秒
                " Session_START(t,INTERVAL '2' second) as wstart,\n" +
                " Session_END(t,INTERVAL '2' second) as sEnd\n" +
                "from sensor\n" +
                //按照会话窗口和id分组
                "group by Session(t,INTERVAL '2' second), id")
                .print();
    }
}
