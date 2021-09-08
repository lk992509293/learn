package com.atguigu.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job实例对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.绑定本地driver或jar
        job.setJarByClass(LogDriver.class);

        //3.绑定mapper和reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //4.设置mapper输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //自定义输出格式
        job.setOutputFormatClass(LogOutputFormat.class);

        //6.绑定输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\java\\develop\\input\\loginput"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\java\\develop\\out\\logout"));

        //7.提交运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
