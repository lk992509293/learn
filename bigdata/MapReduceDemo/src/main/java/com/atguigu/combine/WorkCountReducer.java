package com.atguigu.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WorkCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new IntWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val:values) {
            sum += val.get();
        }
        outV.set(sum);

        //将最终结果写出
        context.write(key, outV);
    }
}
