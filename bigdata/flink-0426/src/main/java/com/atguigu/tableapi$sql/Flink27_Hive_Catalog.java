package com.atguigu.tableapi$sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink27_Hive_Catalog {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.创建HiveCatalog
        String name = "myhive";  // Catalog 名字
        String defaultDatabase = "flink_test"; // 默认数据库
        String hiveConfDir = "D:\\java\\learn\\flink\\hive"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        //TODO 4.注册Catalog
        tableEnv.registerCatalog(name, hive);

        //TODO 5.设置相关参数
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        //TODO 6.设置Sql方言 指定SQL语法为Hive语法
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //TODO 7.查询数据
        tableEnv.executeSql("select * from stu").print();
    }
}
