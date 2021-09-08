package com.atguigu.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.sql.*;

//自定义输出源
public class Flink04_Sink_Custom {
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

        /**
         * TODO：自定义输出到mysql
         */
        sumStream.addSink(new MySink());

        //开启执行任务
        env.execute();

    }

    //自定义sink数据写入mysql
    public static class MySink extends RichSinkFunction<Tuple2<String, Integer>> {
        private Connection conn = null;
        private PreparedStatement pstm = null;

        //用于创建一个mysql连接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop105:3306/test?useSSL=false", "root", "123456");
            pstm = conn.prepareStatement("insert into wordcount values (?,?)");
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            System.out.println("向mysql写入数据");

            pstm.setString(1, value.f0);
            pstm.setInt(2, value.f1);

            //开始执行sql
            pstm.execute();
        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭连接");

            pstm.close();
            conn.close();
        }
    }
}
