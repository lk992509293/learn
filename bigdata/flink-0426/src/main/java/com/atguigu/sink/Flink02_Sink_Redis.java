package com.atguigu.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = keyedStream.sum(1);

        //转换为json字符串
//        SingleOutputStreamOperator<String> mapStream = sum.map(t -> JSON.toJSONString(t));
//        mapStream.print();

        //将数据传输到redis保存
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop105").setPort(6379).build();

        sumStream.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new MyRedisConnect()));

        env.execute();
    }

    public static class MyRedisConnect implements RedisMapper<Tuple2<String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            //return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return JSON.toJSONString(data);
        }
    }
}
