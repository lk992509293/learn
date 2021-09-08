package com.atguigu.UDF;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyFilterFunction<S> {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(4);

        //读取数据
        DataStreamSource<String> inputStream = env.fromElements("hello flink", "hello spark", "java flink");

        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FlinkFilter("flink"));

        filterStream.print();

        env.execute();
    }

    public static class FlinkFilter implements FilterFunction<String> {
        private String keyWord;

        public FlinkFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(String value) throws Exception {
            return value.contains(keyWord);
        }
    }
}
