package com.atguigu.actual_combat;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Flink04_Project_AppAnalysis_By_UnChanel {
    public static void main(String[] args) throws Exception {
        //1.获取流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从自定义数据源中获取数据
        DataStreamSource<MarketingUserBehavior> dataSourceStream = env.addSource(new AppMarketingDataSource());

        //需求分析：分析所有渠道app下载次数
        //将所有数据转换为（广告id，1）的元组形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> behaviorToOneStream = dataSourceStream.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getBehavior(), 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> filterDStream = behaviorToOneStream.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return "download".equals(value.f0);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = filterDStream.keyBy(0);

        keyedStream.sum(1).print();


        //3.开启任务执行
        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );
                ctx.collect(marketingUserBehavior);
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
