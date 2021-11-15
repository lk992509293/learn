package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.atguigu.bean.TableProcess;
import com.atguigu.func.MyStringDebeziumDeserializationSchema;
import com.atguigu.func.TableProcessFunction;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class BaseDbApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //从kafka读取数据
        String topic = "ods_base_db";
        String groupId = "base_db_app_210426";
        DataStreamSource<String> odsDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonDS = odsDS.map(JSON::parseObject);

        //过滤掉控制数据和delete数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });

        //将配置表信息广播流
        DataStreamSource<String> mysqlDS = env.addSource(MySQLSource.<String>builder()
                .hostname("hadoop105")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210426-realtime")
                .tableList("gmall-210426-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build());

        //将配置信息转为广播状态
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mysqlDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcast);

        //侧输出流
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("hbaseTag") {
        };
        SingleOutputStreamOperator<JSONObject> processDS =
                connectDS.process(new TableProcessFunction(mapStateDescriptor, outputTag));

        DataStream<JSONObject> sideOutput = processDS.getSideOutput(outputTag);
        //sideOutput.addSink()
    }
}
