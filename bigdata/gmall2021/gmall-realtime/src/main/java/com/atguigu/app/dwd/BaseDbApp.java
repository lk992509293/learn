package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.MyStringDebeziumDeserializationSchema;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //2.从kafka ods_base_db读取数据，创建主流
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getKafakaConsumer("ods_base_db", "base_db_app"));

        //3.将主流转换为jsonObject对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.map(JSON::parseObject);

        //4.对FlinkCDC抓取数据进行ETL，有用的部分保留，没用的过滤掉
        SingleOutputStreamOperator<JSONObject> filterDStream = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //过滤掉操作类型为delete的数据
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });

        //5.实现动态分流功能
        //5.1使用flinkCDC读取配置信息表
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop105")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> sqlStreamSource = env.addSource(sourceFunction);

        //5.2将配置信息的流转为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = sqlStreamSource.broadcast(mapStateDescriptor);

        //6.将广播流和主流连接起来
        BroadcastConnectedStream<JSONObject, String> connect = filterDStream.connect(broadcast);

        //7.处理连接后的流
        //开启侧输出流，将维度数据写入hbase
        OutputTag<JSONObject> hbaseSideOut = new OutputTag<JSONObject>("hbase-sideOut"){};
        SingleOutputStreamOperator<JSONObject> kafkaOutPut =
                connect.process(new TableProcessFunction(mapStateDescriptor, hbaseSideOut));

        //8.将kafka流数据以及HBase流数据分别写入Kafka和Phoenix
        DataStream<JSONObject> hbaseOutput = kafkaOutPut.getSideOutput(hbaseSideOut);
//        hbaseOutput.print("hbase>>>>>>>>>>>>>>>>");
//        kafkaOutPut.print("kafka>>>>>>>>>>>>>>>>");

        //8.1将数据写入hbase，通过自定义sink的方式把数据写入到Phoenix
        hbaseOutput.addSink(new DimSinkFunction());

        //8.2将数据写入kafka
        kafkaOutPut.addSink(MyKafkaUtil.getKafakaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                //System.out.println("sinkTable = " + element.getString("sinkTable"));
                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));


        //9.开启任务执行
        env.execute();
    }
}
