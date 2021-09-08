package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    @Test
    public void testMkdir() throws IOException, URISyntaxException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();

        //创建hdfs连接地址uri
        String url = "hdfs://hadoop102:8020";

        //创建hdfs连接用户
        String usr = "atguigu";

        //连接hdfs文件系统
        FileSystem fs = FileSystem.get(new URI(url), configuration, usr);

        //2.创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));

        //3.关闭资源
        fs.close();
    }
}
