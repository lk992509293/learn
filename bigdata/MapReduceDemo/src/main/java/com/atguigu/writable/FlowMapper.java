package com.atguigu.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    //创建输出KV
    private Text outK;
    private FlowBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取value中的数据
        String line = value.toString();

        //2.将读取到的字符串按照“\t”分割
        String[] split = line.split("\t");

        //3.提取split字符串数组中的内容
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        //4.设置输出KV
        outK.set(phone);
        outV.setUpFlow(Integer.parseInt(up));
        outV.setDownFlow(Integer.parseInt(down));
        outV.setSumFlow();

        //5.输出context
        context.write(outK, outV);
    }
}
