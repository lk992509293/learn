package com.atguigu.test.app;

import com.atguigu.test.bean.Bean1;
import com.atguigu.test.bean.Bean2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSQLJoin {

    public static void main(String[] args) {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //2.读取端口数据创建流
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop102", 9999);

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<Bean1> bean1DS = ds1.map(line -> {
            String[] fields = line.split(",");
            return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
        });
        SingleOutputStreamOperator<Bean2> bean2DS = ds2.map(line -> {
            String[] fields = line.split(",");
            return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
        });

        //4.将两个流转换为动态表
        tableEnv.createTemporaryView("t1", bean1DS);
        tableEnv.createTemporaryView("t2", bean2DS);

        //5.双流JOIN
        //5.1 inner join     左表：OnCreateAndWrite   右表：OnCreateAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id=t2.id")
//                .print();

        //5.2 left join     左表：OnReadAndWrite      右表：OnCreateAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id=t2.id")
//                .print();

        //5.3 right join    左表：OnCreateAndWrite   右表：OnReadAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 right join t2 on t1.id=t2.id")
//                .print();

        //5.4 full join     左表：OnReadAndWrite     右表：OnReadAndWrite
        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 full join t2 on t1.id=t2.id")
                .print();

    }

}
