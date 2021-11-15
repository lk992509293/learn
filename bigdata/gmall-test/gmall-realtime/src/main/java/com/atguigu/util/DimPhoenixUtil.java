package com.atguigu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

//连接Phoenix
public class DimPhoenixUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //tableName是表名，id就是维度表的主键id
        String redisKey = "DIM:" + tableName + ":" + id;

        //查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {

            //重置过期时间
            jedis.expire(redisKey, 24 * 3600);

            //归还连接
            jedis.close();

            //返回结果
            return JSON.parseObject(dimInfoJsonStr);
        }

        //拼接查询的SQL语句
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        System.out.println(querySQL);
        //查询数据
        List<JSONObject> list = JdbcUtil.queryList(connection, querySQL, JSONObject.class, false);
        JSONObject jsonObject = list.get(0);

        //写入Redis
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 3600);
        jedis.close();

        //返回结果
        return jsonObject;

    }
}
