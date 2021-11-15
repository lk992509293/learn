package com.atguigu.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    //声明redis连接池对象
    public static JedisPool jedisPool = null;

    //获取jedis连接对象
    public static Jedis getJedis() {
        if (jedisPool == null) {
            //获取jedis连接池配置对象
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            //设置jedis最大连接数
            jedisPoolConfig.setMaxTotal(100);
            //连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            //等待时间
            jedisPoolConfig.setMaxWaitMillis(2000);
            //最大闲置连接数
            jedisPoolConfig.setMaxIdle(5);
            //最小闲置连接数
            jedisPoolConfig.setMinIdle(5);
            //取连接的时候进行一下测试 ping pong
            jedisPoolConfig.setTestOnBorrow(true);

            //创建连接池对象
            jedisPool = new JedisPool(jedisPoolConfig, "hadoop105", 6379, 1000);

            //打印
            System.out.println("开辟连接池!");
            return jedisPool.getResource();
        } else {
            System.out.println("连接已存在!");
            return jedisPool.getResource();
        }
    }
}
