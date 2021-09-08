package com.atguigu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountSet {
    public static void main(String[] args) throws Exception{
        //创造执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        DataSet<String> inputDataSet = env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\1.txt");

        //按照空格分词打散，然后统计每个单词出现的次数
        AggregateOperator<Tuple2<String, Integer>>  wordCountDataSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        wordCountDataSet.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String word, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] str = word.split(" ");
            for (String s : str) {
                out.collect(new Tuple2<String, Integer>(s, 1));
            }
        }
    }
}
