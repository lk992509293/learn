package com.atguigu.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行数据
        String line = value.toString();
        
        //2.解析日志
        boolean res = getParaseLog(line);

        //3.日志不合法退出
        if (!res) {
            return;
        }

        //日志合法就写出
        context.write(value, NullWritable.get());
    }

    private boolean getParaseLog(String line) {
        String[] split = line.split(" ");
        if (split.length > 11) {
            return true;
        } else {
            return false;
        }
    }
}
