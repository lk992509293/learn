package com.atguigu.demo02;

import com.atguigu.demo01.HdfsClientUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class CopyFromLocal {
    @Test
    public void testCopyFromLocalFile() throws IOException {
        //1.连接hdfs文件系统
        FileSystem fs = HdfsClientUtil.getFs();

        //创建文件存储路径
        String path = "/xiyou";
        HdfsClientUtil.creatDir(path);

        //2.上传本地文件到hdfs
        fs.copyFromLocalFile(new Path("D:\\tangsen.txt"), new Path(path));

        //3.关闭连接
        fs.close();
    }


}
