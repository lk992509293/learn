package com.atguigu.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

//批处理wordcount
public class WordCount_Batch_Test01 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        //2.从文件中读取数据
        DataSource<String> dataSource = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink-0426\\input\\word.txt");
        
        //3.转换数据格式(匿名内部类的方式)
/*        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne  = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    //out.collect(Tuple2.of(word, 1));
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });*/

        //3.转换数据格式(继承接口，实现方法)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = dataSource.flatMap(new MyFlatMapper());

        //4.按照单词分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneUG = wordAndOne.groupBy(0);

        //5.计算相同的单词出现的次数
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = wordAndOneUG.sum(1);

        //6.打印输出结果
        aggregateOperator.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] str = line.split(" ");
            for (String word : str) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
