package com.atguigu.environment;

import com.atguigu.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从自定义输出源中读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        //打印输出
        dataStream.print();

        //执行任务
        env.execute();
    }

    //实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {
        //定义一个标识位来控制run方法启停
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //顶一个随机数发生器
            Random random = new Random();

            //设置十个温度传感器
            HashMap<String, Double> map = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                map.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensor_Id : map.keySet()) {
                    double newTemp = map.get(sensor_Id) + random.nextGaussian();
                    map.put(sensor_Id, newTemp);
                    sourceContext.collect(new SensorReading(sensor_Id, System.currentTimeMillis(), newTemp));
                }
            }

            //控制输出频率
            Thread.sleep(1000);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
