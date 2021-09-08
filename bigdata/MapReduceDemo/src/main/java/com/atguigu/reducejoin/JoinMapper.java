package com.atguigu.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {
    private Text outK;
    private JoinBean outV;
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new JoinBean();

        InputSplit inputSplit = context.getInputSplit();
        FileSplit split = (FileSplit)inputSplit;
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取一行数据
        String line = value.toString();

        //2.判断是哪个文件，对不同的文件分别处理
        if (fileName.contains("order")) {
            String[] split = line.split("\t");
            //封装outK
            outK.set(split[1]);

            //封装outV
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else {
            String[] split = line.split("\t");
            //封装outK
            outK.set(split[0]);

            //封装outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }

        //写出KV
        context.write(outK, outV);
    }
}
