package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

//直接查询Phoenix
public class DimUtil {
    public static JSONObject getDimInfo(Connection conn, String tableName, String id) throws Exception {
        //拼接redis的key，此处使用String存储
        String redisKey = "DIM:" + tableName + ":" + id;

        //查询redis
        Jedis jedis = RedisUtil.getJedis();
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {
            //数据已经缓存到redis中
            //更新过期时间
            jedis.expire(redisKey, 24 * 3600);

            //关闭连接
            jedis.close();

            //返回查询到的数据
            return JSON.parseObject(dimInfoJsonStr);
        }

        //拼接查询的SQL语句
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        //打印
        System.out.println("querySQL = " + querySQL);

        //获取查询语句
        List<JSONObject> jsonObjects = JdbcUtil.queryList(conn, querySQL, JSONObject.class, false);

        //返回结果，由于只有一条数据，所以返回第一条就可以
        JSONObject jsonObject = jsonObjects.get(0);

        //将查询到的结果写入redis缓存
        jedis.set(redisKey, jsonObject.toJSONString());
        //设置过期时间为1天
        jedis.expire(redisKey, 24 * 3600);
        //关闭连接
        jedis.close();

        return jsonObject;
    }

    //清除redis缓存
    public static void deleteCach(String tableName, String id) {
        String redisKey = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        //通过key清除缓存
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        JSONObject dim_base_trademark = getDimInfo(connection, "DIM_BASE_TRADEMARK", "12");
        long second = System.currentTimeMillis();
        JSONObject dim_base_trademark1 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "12");
        long end = System.currentTimeMillis();

        System.out.println(second - start);
        System.out.println(end - second);

        System.out.println(dim_base_trademark);
        System.out.println(dim_base_trademark1);

        connection.close();
    }
}
