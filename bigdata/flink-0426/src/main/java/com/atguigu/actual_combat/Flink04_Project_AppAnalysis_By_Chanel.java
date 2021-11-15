package com.atguigu.actual_combat;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        //1.获取流式数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从自定义数据源中读取数据
        DataStreamSource<MarketingUserBehavior> dataSourceStream = env.addSource(new Flink04_Project_AppAnalysis_By_UnChanel.AppMarketingDataSource());

        //需求分许：统计不同渠道app下载数量
        //将数据转换为（渠道+行为，1）二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDStream = dataSourceStream.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1);
            }
        });

        mapDStream.keyBy(0).sum(1).print();

        //3.开启任务执行
        env.execute();
    }
}
