package com.atguigu.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson.JSON;

import java.util.Properties;

public class Flink02_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        SingleOutputStreamOperator<String> mapStream = sum.map(JSON::toJSONString);

        //作为生产者向kafka传递数据
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","hadoop105:9092");
//        properties.setProperty("group.id","test");
//        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
//                "sensor",
//                new SimpleStringSchema(),
//                properties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//        );


        mapStream.addSink(new FlinkKafkaProducer<String>("hadoop105:9092", "sensor", new SimpleStringSchema()));

        env.execute();
    }
}
