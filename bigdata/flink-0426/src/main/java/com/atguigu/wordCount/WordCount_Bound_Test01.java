package com.atguigu.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//计算无界流
public class WordCount_Bound_Test01 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置全局并行度
        env.setParallelism(1);

        //3.从socket数据流中实时读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //4.对数据格式进行转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne  = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    //out.collect(new Tuple2<>(word, 1));
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //5.对数据按照相同单词分组
        /*KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });*/
        //5.对数据按照相同单词分组(使用lambda表达式)
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(t -> t.f0);


        //6.累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.sum(1);

        //7.打印计算结果
        res.print();

        //8.开启执行任务
        env.execute();
    }
}
