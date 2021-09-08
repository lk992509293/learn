package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object key, byte[] keys, Object value, byte[] values, Cluster cluster) {
        //获取消息
        String s1 = value.toString();

        //创建partition
        int partition = 0;
        if (s1.contains("atguigu")) {
            partition = 1;
        } else {
            partition = 0;
        }
        //返回分区号
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
