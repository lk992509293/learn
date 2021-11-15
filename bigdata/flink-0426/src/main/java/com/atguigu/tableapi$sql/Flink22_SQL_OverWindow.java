package com.atguigu.tableapi$sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink22_SQL_OverWindow {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

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

        //TODO 使用Over开窗
//        tableEnvironment.executeSql("select\n" +
//                " id,\n" +
//                " ts,\n" +
//                " vc,\n" +
//                " sum(vc) over(partition by id order by t) \n" +
//                "from sensor")
//                .print();

        tableEnvironment.sqlQuery("select " +
                "id," +
                "vc," +
                "count(vc) over w, " +
                "sum(vc) over w " +
                "from sensor " +
                "window w as (partition by id order by t)"
        ).execute()
                .print();
    }
}
