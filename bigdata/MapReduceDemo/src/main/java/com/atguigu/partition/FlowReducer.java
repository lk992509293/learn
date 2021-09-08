package com.atguigu.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new FlowBean();
    }

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int totalUp = 0;
        int totalDown = 0;

        //1.提取输入集合values
        for (FlowBean val : values) {
            totalUp += val.getUpFlow();
            totalDown += val.getDownFlow();
        }

        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //2.输出
        context.write(key, outV);
    }
}
