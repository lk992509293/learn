package com.atguigu.redis;

import redis.clients.jedis.Jedis;

public class Jedis_Test {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.10.105", 6379);

        //执行ping命令
        String ping = jedis.ping();

        System.out.println(ping);

        jedis.close();
    }
}
