package com.atguigu.demo02;

import com.atguigu.demo01.HdfsClientUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class CopyToLocalFile {
    @Test
    public void testCopyToLocalFile() throws IOException {
        //1.连接hdfs文件系统
        FileSystem fs = HdfsClientUtil.getFs();

        //2.执行下载操作
        fs.copyToLocalFile(false, new Path("/sanguo/shuguo.txt"), new Path("D:\\"));

        //3.关闭连接
        HdfsClientUtil.close();
    }
}
