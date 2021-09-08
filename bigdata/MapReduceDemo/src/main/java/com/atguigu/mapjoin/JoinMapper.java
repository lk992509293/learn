package com.atguigu.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private HashMap<String, String> mapPd = new HashMap<>();
    Text text = new Text();

    //在数据开始阶段，直接将小表阶段将数据添加到缓存
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //通过缓存文件得到小表数据
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //通过字符缓冲流按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        //逐行读取，按行处理
        String line;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\t");
            mapPd.put(split[0], split[1]);
        }

        //关闭流
        IOUtils.closeStream(reader);
        IOUtils.closeStream(fis);
        fs.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取大表数据
        String line = value.toString();

        //通过pid取出每行的pname
        String[] split = line.split("\t");
        String pname = mapPd.get(split[1]);

        //将大表中的pid换成pname
        text.set(split[0] + "\t" + pname + "\t" + split[2]);

        //输出
        context.write(text, NullWritable.get());
    }
}
