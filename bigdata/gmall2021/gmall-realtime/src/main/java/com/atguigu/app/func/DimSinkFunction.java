package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
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
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //反射方式注册驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);

        //通过ClassLoader注册驱动
        //ClassLoader.getSystemClassLoader().loadClass(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // value:{"database":"gmall-210426-flink","tableName":"base_trademark","after":""....}
    //组件sql语句，将数据写入
    //如果未读数据发生了变化，则需要清除redis中失效的数据
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        System.out.println("value.toJSONString() = " + value.toJSONString());

        try {
            String upsertSQL = getUpsertSQL(value.getString("sinkTable"), value.getJSONObject("after"));

            //打印sql语句
            System.out.println("upsertSQL = " + upsertSQL);

            //预编译sql
            preparedStatement = conn.prepareStatement(upsertSQL);

            //如果该数据为更新类型的数据，则要先删除redis内的缓存
            if ("update".equals(value.getString("type"))) {
                DimUtil.deleteCach(value.getString("sinkTable").toUpperCase(),
                        value.getJSONObject("after").getString("id"));
            }

            //执行sql
            preparedStatement.execute();

            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String getUpsertSQL(String sinkTable, JSONObject after) {
        //获取after key字段
        Set<String> cols = after.keySet();
        //获取value字段
        Collection<Object> values = after.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(cols, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
