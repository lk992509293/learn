package com.atguigu.sink;

import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink05_Sink_JDBC {
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = keyedStream.sum(1);

        /*通过jdbc把数据发送到数据库*/
        sumStream.addSink(JdbcSink.sink(
                "insert into wordcount values (?,?)",
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setInt(2, t.f1);
                },
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://hadoop105:3306/test?useSSL=false")
                .withUsername("root")
                .withPassword("123456")
                //指定全类名
                .withDriverName(Driver.class.getName()).build()
        ));

        env.execute();
    }
}
