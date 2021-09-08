package com.atguigu.partionCompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    private FlowBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new FlowBean();
    }

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历集合，便面总流量相同的情况
        for (Text value : values) {
            //将KV调换位置
            context.write(value, key);
        }
    }
}
