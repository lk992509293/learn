package com.atguigu.tableapi$sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink13_SQL_EventTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Configuration configuration = tableEnvironment.getConfig().getConfiguration();
        configuration.setString("table.local-time-zone", "GMT");

        //TODO 3.在建表时指定事件时间
        //as表示调用to_timestamp函数
        tableEnvironment.executeSql(("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                //t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')对t做格式化操作，格式化为timestamp(3) 类型
                //(ts/1000)是把时间转为秒
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                //watermark for t as t - interval '5' second指定watermark，t - interval '5' t减去时间间隔5秒，类似于指定乱序程度。
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"

        ));

//        5.	当发现时区所导致的时间问题时，可设置本地使用的时区：
//        Configuration configuration = tableEnv.getConfig().getConfiguration();
//        configuration.setString("table.local-time-zone", "GMT");


        tableEnvironment.executeSql("select * from sensor").print();
    }
}
