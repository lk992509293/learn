package com.atguigu.tableapi$sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

//将表数据输出到kafka
public class Flink06_TableAPI_Sink_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //转流为表
        Table table = tableEnvironment.fromDataStream(dataStreamSource);

        //查询表中的数据
        Table resultTable = table.where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //将数据写入kafka
        Schema schema = new Schema()
                .field("id", "String")
                .field("ts", "BIGINT")
                .field("vc", "Integer");

        tableEnvironment.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .sinkPartitionerRoundRobin()
                .property("bootstrap.servers", "hadoop105:9092,hadoop106:9092,hadoop107:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        resultTable.executeInsert("sensor");
    }
}
