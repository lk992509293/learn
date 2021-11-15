package com.atguigu.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    //声明Phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取Phoenix驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //获取连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //输入数据格式：value:{"database":"gmall-210426-flink","tableName":"base_trademark","after":""....}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            //获取数据
            JSONObject after = value.getJSONObject("after");
            String tableName = value.getString("sinkTable");

            //创建sql语句，将新数据写入sinkTable中
            String upsertSQL = genUpsertSQL(tableName, after);

            //打印sql语句
            System.out.println("upsertSQL = " + upsertSQL);

            //预编译sql
            preparedStatement = connection.prepareStatement(upsertSQL);

            //执行写入操作
            preparedStatement.execute();

            //比较一下
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //关闭资源
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSQL(String tableName, JSONObject after) {
        Set<String> columns = after.keySet();
        Collection<Object> values = after.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" +
                StringUtils.join(columns, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
