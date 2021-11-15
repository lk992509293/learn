package com.atguigu.tableapi$sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_Connector_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //创建表的元信息
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());


        //用表环境连接kafka
        tableEnvironment.connect(
                new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .property("group.id", "bigdata")
                        .property("bootstrap.servers", "hadoop105:9092,hadoop106:9092,hadoop107:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //对表进行动态查询
        Table sensorTable = tableEnvironment.from("sensor");
        Table resultTable = sensorTable
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        //把动态表转换为流
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnvironment.toRetractStream(resultTable, Row.class);
        tuple2DataStream.print();

        env.execute();

    }
}
