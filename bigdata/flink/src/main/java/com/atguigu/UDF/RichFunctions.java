package com.atguigu.UDF;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctions {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\2.txt");

        //转换成SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();

        env.execute();

    }

    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>>{

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) {
            //初始化工作，一般是定义状态，或者连接新的数据库
            System.out.println("open");
        }

        @Override
        public void close() {
            //一般是关闭连接和清空操作
            System.out.println("close");
        }
    }
}
