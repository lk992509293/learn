package com.atguigu.mapjoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class JoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.绑定当前driver或者jar
        job.setJarByClass(JoinDriver.class);

        //3.绑定mapper
        job.setMapperClass(JoinMapper.class);

        //4.绑定mapper的输出KV
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.绑定最终的输出kv
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.加载缓存数据
        job.addCacheFile(new URI("file:///D:/java/develop/input/cache/pd.txt"));
        // Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        //7.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\java\\develop\\input\\join"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\java\\develop\\out\\mapjoin"));

        //8.运行提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
