package com.atguigu.tableapi$sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink03_Connector_FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 1.创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2.创建表
        //1.创建表的元数据
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        //2.连接文件，并创建一个临时表
        tableEnvironment.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //3.做成表对象，对表进行动态查询
        Table table = tableEnvironment.from("sensor");
        Table resultTable = table.groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        //4.把动态表转换为流
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnvironment.toRetractStream(resultTable, Row.class);

        tuple2DataStream.print();

        env.execute();


    }
}
