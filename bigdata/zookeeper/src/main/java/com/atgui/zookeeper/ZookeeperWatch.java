package com.atgui.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ZookeeperWatch {
    private ZooKeeper zooKeeper;

    //删除非空节点
    @Test
    public void testDeleteAll() throws KeeperException, InterruptedException {
        deleteAll("/xiyouji");
    }

    //删除非空节点
    public void deleteAll(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }
        //现获取所有的子节点
        List<String> children = zooKeeper.getChildren(path, false);
        for (String child : children) {
            deleteAll(path + "/" + child);
        }
        zooKeeper.delete(path, stat.getVersion());
    }

    //删除空节点
    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/xiyouji", false);
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }

        zooKeeper.delete("/xiyouji", stat.getVersion());
    }

    //设置节点的值
    @Test
    public void set() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/xiyouji", false);
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }
        Stat data = zooKeeper.setData("/xiyouji", "wuchengen".getBytes(), -1);
    }

    //获取子节点的数据，并监听
    @Test
    public void getAndWatch() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/xiyouji", false);
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }
        byte[] data = zooKeeper.getData("/xiyouji", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event = " + event);
            }
        }, stat);
        System.out.println(new String(data));

        //线程阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    //获取子节点的数据，不监听
    @Test
    public void get() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/xiyouji", false);
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }
        byte[] data = zooKeeper.getData("/xiyouji", false, stat);
        System.out.println(new String(data));
    }

    //创建子节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String path = zooKeeper.create("/xiyouji", "sunwukong".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        System.out.println("path = " + path);
        Thread.sleep(Long.MAX_VALUE);
    }

    //获取子节点列表并且不监听
    @Test
    public void lsAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event = " + event);
            }
        });
        System.out.println("children = " + children);

        //因为设置了监听。所以程序不能结束
        Thread.sleep(Long.MAX_VALUE);
    }

    //获取子节点列表
    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", false);
        System.out.println("children = " + children);
    }

    @Before
    public void init() throws IOException {
        //获取客户端对象
        String conString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;
        zooKeeper = new ZooKeeper(conString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    //关闭客户端
    @After
    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
