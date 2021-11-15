package com.atguigu.tableapi$sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink19_SQL_GroupWindow_Tumble {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //通过sql语句从文件系统获取数据
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

        //TODO 开启一个滚动窗口
        tableEnvironment.executeSql("select \n" +
                " id, \n" +
                " sum(vc) vcSum, \n" +
                //TUMBLE_START设置窗口的开始时间为t，窗口的大小为3秒
                " TUMBLE_START(t,INTERVAL '3' second) as startWindow, \n" +
                " TUMBLE_END(t,INTERVAL '3' second) as endWindow \n" +
                "from sensor \n" +
                //按照滚动窗口分组，窗口的大小为3秒
                "group by TUMBLE(t,INTERVAL '3' second), id")
                .print();
    }
}
