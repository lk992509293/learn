package com.atguigu.demo02;

import com.atguigu.demo01.HdfsClientUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class Rename {
    @Test
    public void testRename() throws IOException {
        //1.连接hdfs文件系统
        FileSystem fs = HdfsClientUtil.getFs();

        //2.修改文件名称
        fs.rename(new Path("/sanguo"), new Path("/sanguozhi"));

        //3.关闭连接
        fs.close();
    }
}
