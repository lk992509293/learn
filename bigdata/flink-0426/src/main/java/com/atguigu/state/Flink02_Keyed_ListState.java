package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink02_Keyed_ListState {
    //需求：针对每个传感器输出最高的3个水位值
    public static void main(String[] args) throws Exception {
        //1.获取流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        //3.将数据个数转换为Javabean
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
            }
        });

        //4.按照id分组
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //5.对数据排序，并取其前三个数据
        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //1.定义状态
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getIterationRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //TODO 使用状态保存数据
                listState.add(value.getVc());

                //创建list集合用来保存状态中的数据
                ArrayList<Integer> list = new ArrayList<>();

                for (Integer lastVc : listState.get()) {
                    list.add(lastVc);
                }

                //对集合中的元素排序
                list.sort((v1, v2) -> v2 - v1);

                //如果list中的数据大于三条就把第四条数据删除
                if (list.size() > 3) {
                    list.remove(3);
                }

                //将最大的三条数据存放到list中
                listState.update(list);
                out.collect(list.toString());
            }
        });

        process.print();

        env.execute();
    }
}
