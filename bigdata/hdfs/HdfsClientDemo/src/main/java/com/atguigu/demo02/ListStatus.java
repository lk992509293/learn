package com.atguigu.demo02;

import com.atguigu.demo01.HdfsClientUtil;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;

public class ListStatus {
    @Test
    public void test() throws IOException {
        FileSystem fs = HdfsClientUtil.getFs();
        Path path = new Path("/");
        listStatus(fs, path);
        fs.close();
    }

    public void listStatus(FileSystem fs, Path path) throws IOException {

        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件名：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录名：" + fileStatus.getPath().getName());
                listStatus(fs, fileStatus.getPath());
            }
        }
    }
}
