package com.atguigu.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkCDC_SQL {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2获取表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //3.使用DDL的方式创建一个临时表
        tableEnvironment.executeSql("CREATE TABLE base_trademark( " +
                " id INT, " +
                " tm_name STRING, " +
                " logo_url STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = 'hadoop105', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'gmall-flink', " +
                " 'table-name' = 'base_trademark' " +
                ")");

        //4.查询表数据
        Table table = tableEnvironment.sqlQuery("select * from base_trademark");


        //5.输出数据，采用撤回流
        DataStream<Tuple2<Boolean, Row>> result = tableEnvironment.toRetractStream(table, Row.class);
        result.print();

        //6.开启执行
        env.execute();
    }
}
