package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.func.DimSinkFunction;
import com.atguigu.func.MyStringDebeziumDeserializationSchema;
import com.atguigu.func.TableProcessFunction;
import com.atguigu.util.MyKafkaUtil;
import org.apache.commons.math3.analysis.solvers.BracketedUnivariateSolver;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.设置状态后端和检查点
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop105:8020"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 3.从kafka读取数据
        //创建消费者和生产者topic，创建消费者组
        String ods_db_source = "ods_base_db";
        String groupId = "ods_db_group";

        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ods_db_source, groupId));
        //dataStreamSource.print("kafka>>>>");

        //TODO 4.将数据格式转为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonDS = dataStreamSource.map(JSON::parseObject);

        //TODO 5.过滤掉其中的null值数据和操作类型为delete的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(json -> {
            return json != null && !"delete".equals(json.getString("type"));
        });

        //TODO 6.通过flinkCDC从mysql读取配置信息
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
        DataStreamSource<String> propertyDS = env.addSource(sourceFunction);
//        propertyDS.print("broadcast>>>>>");

        //TODO 7.将配置信息流转换为广播流
        //创建广播规则
        MapStateDescriptor<String, TableProcess> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState", String.class, TableProcess.class);
        //根据广播规则创建广播流
        BroadcastStream<String> broadcastDS = propertyDS.broadcast(ruleStateDescriptor);

        //TODO 8.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        //TODO 9.对连接后的流进行处理
        //本需求的根本目的就是根据配置信息，将维度数据输出到hbase，将事实数据输出到kafka，事实数据作为主流输出，维度数据作为侧输出流，
        // 由于维度数据要输出到表中，所以要和表的字段对应，因此输出一个json与表的字段对应
        OutputTag<JSONObject> broadcastSide = new OutputTag<JSONObject>("hbaseTag"){};

        //处理主流数据和广播流数据，主流数据根据广播流规则进行处理，定义一个TableProcessFunction类去处理主流和广播流
        SingleOutputStreamOperator<JSONObject> kafkaDS = connectDS.process(new TableProcessFunction(ruleStateDescriptor, broadcastSide));
        kafkaDS.print("kafka>>>>");

        //TODO 10.将主流数据输出到kafka
        kafkaDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"), element.getString(
                        "after").getBytes());
            }
        }));

        //使用自定义sink输出到hbase
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(broadcastSide);
        //hbaseDS.print("hbase>>>>");
//        hbaseDS.addSink(new DimSinkFunction());

        //TODO 开启任务执行
        env.execute();

    }
}
