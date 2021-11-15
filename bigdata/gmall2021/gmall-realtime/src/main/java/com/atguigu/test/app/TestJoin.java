package com.atguigu.test.app;

import com.atguigu.test.bean.Bean1;
import com.atguigu.test.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TestJoin {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop102", 9999);

        //TODO 在 Flink 程序中可以直接使用 ParameterTool.fromArgs(args) 获取到所有的参数，也可以通过 parameterTool.get
        // ("username") 方法获取某个参数对应的值。
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //TODO 读取系统属性
        ParameterTool parameterTool1 = ParameterTool.fromSystemProperties();
        System.out.println("parameterTool = " + parameterTool);

        //TODO 在 ExecutionConfig 中可以将 ParameterTool 注册为全作业参数的参数，这样就可以被 JobManager 的web 端以及用户⾃定义函数中以配置值的形式访问。
        // 可以不用将ParameterTool当作参数传递给算子的自定义函数，直接在用户⾃定义的 Rich 函数中直接获取到参数值了。
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<Bean1> bean1DS = ds1.map(line -> {
            String[] fields = line.split(",");
            return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
        });
        SingleOutputStreamOperator<Bean2> bean2DS = ds2.map(line -> {
            String[] fields = line.split(",");
            return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
        });

        //4.提取事件时间生成WaterMark
        SingleOutputStreamOperator<Bean1> bean1WithWMDS = bean1DS.assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
            @Override
            public long extractTimestamp(Bean1 element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        SingleOutputStreamOperator<Bean2> bean2WithWMDS = bean2DS.assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
            @Override
            public long extractTimestamp(Bean2 element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //5.双流JOIN
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> result = bean1WithWMDS.keyBy(Bean1::getId)
                .intervalJoin(bean2WithWMDS.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 left, Bean2 right, Context ctx, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        //6.打印数据
        result.print();

        //7.启动任务
        env.execute();

    }

}
