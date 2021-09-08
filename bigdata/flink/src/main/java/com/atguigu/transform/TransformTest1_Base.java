package com.atguigu.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(1);

        //从文件中读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\1.txt");

        //1.map将String转换为k-v对输出
        DataStream<Object> mapStream = inputStream.map(new MapFunction<String, Object>() {
            @Override
            public HashMap<String, Integer> map(String line) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(line, line.length());
                return map;
            }
        });

        //2.flatMap,按逗号分割
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for( String field: fields )
                    out.collect(field);
            }
        });

        //3.filter过滤
        DataStream<String> filterStream = inputStream.filter(line -> line.startsWith("sensor_1"));

        //打印输出
        mapStream.print();
        flatMapStream.print();
        filterStream.print();

        env.execute();
    }
}
