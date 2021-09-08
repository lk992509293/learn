package com.atguigu.demo02;

import com.atguigu.demo01.HdfsClientUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ListFiles {
    @Test
    public void testListFiles() throws IOException {
        FileSystem fs = HdfsClientUtil.getFs();

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            System.out.println(next.getPermission());
            System.out.println(Arrays.toString(next.getBlockLocations()));
            System.out.println(next.getBlockSize());
            System.out.println(next.getGroup());
            System.out.println(next.getModificationTime());
        }
        fs.close();
    }
}
