package com.atguigu.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件获取job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.绑定当前Driver类或者Jar包
        job.setJarByClass(FlowDriver.class);//使用类名.class获取class对象
        //使用反射获取Class对象
        //job.setJarByClass(Class.forName("com.atguigu.partition.FlowDriver"));

        //3.绑定当前mr的mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.绑定当前mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.绑定最终的输入输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //8.指定自定义分区
        job.setPartitionerClass(ProvincePartitioner.class);

        //9.指定ReduceTask分区数量
        job.setNumReduceTasks(5);

        //6.绑定输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\java\\develop\\input\\partionflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\java\\develop\\out\\outpartion"));

        //7.提交运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
