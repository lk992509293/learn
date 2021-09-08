package com.atguigu.combineTeInFor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WorkCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件获取job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.绑定当前的driver类或者是jar
        job.setJarByClass(WorkCountDriver.class);

        //3.绑定当前mr的mapper和reducer
        job.setMapperClass(WorkCountMapper.class);
        job.setReducerClass(WorkCountReducer.class);

        //4.指定mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定使用combineinputformat
        job.setInputFormatClass(CombineTextInputFormat.class);

        //指定虚拟切片的大小为4M
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        //指定虚拟切片大小为20M
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        //6.指定程序的输入路径
        FileInputFormat.setInputPaths(job, new Path("D:\\java\\develop\\input\\inputcombineText"));

        //7.指定程序的输出路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\java\\develop\\out\\outformat2"));

        //8.提交运行  如果是true 打印的日志会多一点，方法返回值 如果成功是true 如果失败了是false
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
