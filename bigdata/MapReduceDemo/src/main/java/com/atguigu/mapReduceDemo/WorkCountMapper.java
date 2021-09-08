package com.atguigu.mapReduceDemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WorkCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK;
    private IntWritable outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new IntWritable(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取输入
        String s = value.toString();

        //2.以空格分割
        String[] str = s.split(" ");

        //3.将获得的字符串输出
        for (String word : str) {
//            Text outK = new Text(word);
//            IntWritable outV = new IntWritable(1);
            outK.set(word);
            context.write(outK, outV);
        }
    }
}

