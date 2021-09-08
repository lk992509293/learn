package com.atguigu.demo02;

import com.atguigu.demo01.HdfsClientUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class DelDir {
    @Test
    public void testDelete() throws IOException {
        FileSystem fs = HdfsClientUtil.getFs();

        fs.delete(new Path("/xiyou"), true);

        fs.close();
    }
}
