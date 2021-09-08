package com.atguigu.transform;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置整体并行度
        env.setParallelism(1);

        //读取传感器温度数据
        DataStream<String> inputStream= env.readTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\flink\\input\\2.txt");

        //对输入流数据做拆分
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //按照id分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        //聚合，取最小的温度值，并返回当前最新时间戳
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        //打印输出
        resultStream.print();

        //执行作业
        env.execute();
    }
}
