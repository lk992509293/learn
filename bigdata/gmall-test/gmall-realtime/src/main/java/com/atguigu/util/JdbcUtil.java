package com.atguigu.util;

import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    //查询Phoenix维度数据
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clazz,
                                        boolean underScoreToCamel) throws Exception {
        //1.创建集合用于存放结果数据
        ArrayList<T> resList = new ArrayList<>();

        //2.预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //3.执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //获取查询结果元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        //获取查询结果列数量
        int columnCount = metaData.getColumnCount();

        //4.遍历resultSet，给每行数据封装泛型对象并将其加入结果集中
        while (resultSet.next()) {
            //获取泛型对象
            T t = clazz.newInstance();

            //将查询到的结果数据逐条封装到list中
            for (int i = 1; i <= columnCount; i++) {
                //获取列名
                String columnName = metaData.getCatalogName(i);
                BeanUtils.setProperty(t, columnName, resultSet.getObject(i));
            }
            resList.add(t);
        }
        //关闭资源
        preparedStatement.close();
        resultSet.close();

        //返回查询结果
        return resList;
    }
}

