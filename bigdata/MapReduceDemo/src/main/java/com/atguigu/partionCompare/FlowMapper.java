package com.atguigu.partionCompare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    //创建输出KV
    private FlowBean outK;
    private Text outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new FlowBean();
        outV = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.从value中读取数据
        String line = value.toString();

        //2.按照“\t”分割数据
        String[] split = line.split("\t");

        //3.封装outK, outV
        int up = Integer.parseInt(split[1]);
        int down = Integer.parseInt(split[2]);

        outK.setUpFlow(up);
        outK.setDownFlow(down);
        outK.setSumFlow();
        outV.set(split[0]);

        context.write(outK, outV);
    }
}
