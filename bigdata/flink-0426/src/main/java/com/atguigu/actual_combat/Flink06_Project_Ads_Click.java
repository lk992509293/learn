package com.atguigu.actual_combat;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.TUPLE;

public class Flink06_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文件中读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/AdClickLog.csv");

        //需求分析：统计各省广告点击实时次数
        //转换数据结构为javabean
        SingleOutputStreamOperator<AdsClickLog> adsClickStream = dataStreamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String line) throws Exception {
                String[] split = line.split(",");
                return new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //将数据转换为元组格式
        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Integer>> provinceWithAdsToOne = adsClickStream.map(new MapFunction<AdsClickLog, Tuple2<Tuple2<String, Long>, Integer>>() {
            @Override
            public Tuple2<Tuple2<String, Long>, Integer> map(AdsClickLog value) throws Exception {
                return Tuple2.of(Tuple2.of(value.getProvince(), value.getAdId()), 1);
            }
        });

        provinceWithAdsToOne.keyBy(0).sum(1).print();

        //3.开启执行任务
        env.execute();
    }
}
