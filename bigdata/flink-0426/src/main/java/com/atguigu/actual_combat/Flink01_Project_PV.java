package com.atguigu.actual_combat;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.parsers.SAXParser;

public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        //1.获取流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(4);

        //2.读取文件数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");

        //需求分析：统计网站总浏览量（PV），就是统计网站被访问的次数,使用javabean封装数据
        SingleOutputStreamOperator<UserBehavior> userBehavior = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String line) throws Exception {
                //1.对输入数据按照“，”切分
                String[] split = line.split(",");
                return new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //过滤出属于访问pv的数据
        SingleOutputStreamOperator<UserBehavior> filterDStream = userBehavior.filter(userInfo -> "pv".equals(userInfo.getBehavior()));

        //将数据类型转换为tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvDStream = filterDStream.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                //return Tuple2.of("pv", 1);
                return new Tuple2<>("pv", 1);
            }
        });

        //统计访问次数,先按照key分组，然后再对第二个字段求和
        pvDStream.keyBy(t -> t.f0).sum(1).print();

        //3.开启执行任务
        env.execute();
    }
}
