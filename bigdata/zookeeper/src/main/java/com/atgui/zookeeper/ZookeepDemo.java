package com.atgui.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZookeepDemo {
    public static void main(String[] args) throws InterruptedException, IOException {
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;
        ZooKeeper zoo = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event = " + event);
            }
        });

        //关闭客户端对象
        zoo.close();
    }
}
