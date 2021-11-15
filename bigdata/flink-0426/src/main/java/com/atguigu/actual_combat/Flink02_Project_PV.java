package com.atguigu.actual_combat;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_Project_PV {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //2.从文件中读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");

        //需求分析：统计网站总浏览量（PV），就是统计网站被访问的次数,使用javabean封装数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
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

        //过滤出pv行为
        SingleOutputStreamOperator<UserBehavior> filterDStream = userBehaviorDStream.filter(userInfo -> "pv".equals(userInfo.getBehavior()));

        //按照行为类型分组
        KeyedStream<UserBehavior, String> keyedStream = filterDStream.keyBy(UserBehavior::getBehavior);

        //使用process对数据进行累加统计
        keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Tuple2<String, Integer>>() {
            private int count = 0;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                count++;
                out.collect(new Tuple2<>(value.getBehavior(), count));
            }
        }).print();

        //3.开启执行任务
        env.execute();
    }
}
