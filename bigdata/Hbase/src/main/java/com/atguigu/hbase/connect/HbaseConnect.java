package com.atguigu.hbase.connect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnect {
    public static void main(String[] args) throws IOException {
        //1.获取配置连接
        Configuration conf = HBaseConfiguration.create();

        //2.给配置类添加配置
        conf.set("hbase.zookeeper.quorum", "hadoop105,hadoop106,hadoop107");

        //3.获取连接
        Connection conn = ConnectionFactory.createConnection(conf);

        //打印连接
        System.out.println("conn = " + conn);

        //4.关闭连接
        conn.close();
    }
}
