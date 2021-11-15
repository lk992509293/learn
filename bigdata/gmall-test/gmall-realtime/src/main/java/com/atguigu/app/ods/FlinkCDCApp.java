package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.func.MyStringDebeziumDeserializationSchema;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 指定状态后端并开启CK
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //3.从mysql读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop105")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
        dataStreamSource.print("kafka>>>>");

        //4.将数据写入kafka
        dataStreamSource.addSink(MyKafkaUtil.getFlinkKafkaProducer("ods_base_db"));

        //5.开启执行任务
        env.execute();
    }
}
