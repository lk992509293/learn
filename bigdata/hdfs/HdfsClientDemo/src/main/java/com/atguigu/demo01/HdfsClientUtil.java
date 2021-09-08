package com.atguigu.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientUtil {
    private static FileSystem fs;

    private HdfsClientUtil() {
    }

    static {
        //1.获取文件系统
        Configuration con = new Configuration();

        //创建HDFS的连接地址
        String url = "hdfs://hadoop102:8020";

        //创建用户
        String usr = "atguigu";

        //连接hdfs文件系统
        try {
            fs = FileSystem.get(new URI(url), con, usr);
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void creatDir(String path) {
        try {
            fs.mkdirs(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static FileSystem getFs() {
        if (fs != null) {
            return fs;
        }
        throw new RuntimeException("连接文件系统失败");
    }

    public static void close() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
