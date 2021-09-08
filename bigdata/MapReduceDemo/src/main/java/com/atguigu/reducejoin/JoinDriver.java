package com.atguigu.reducejoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.绑定当前driver或者jar
        job.setJarByClass(JoinDriver.class);

        //3.绑定mapper和reducer
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReduce.class);

        //4.绑定mapper输出KV
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        //5.绑定最终输出KV
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.绑定输入输出文件
        FileInputFormat.setInputPaths(job, new Path("D:\\java\\develop\\input\\join"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\java\\develop\\out\\join02"));

        //7.提交运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
