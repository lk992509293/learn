package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//使用jdbc连接的都通用
public class JdbcUtil {
    /**
     * @Author:lk
     * @Description:TODO
     * @DateTime:2021/9/25 16:58
     * @Params:*
     * @param conn
     * @param querySQL
     * @param clazz
     * @param underScoreToCamel 当值为true时，就把下划线风格参数转换为小驼峰风格
     * @Return:java.util.List<T>
     */
    public static <T> List<T> queryList(Connection conn, String querySQL, Class<T> clazz, boolean underScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        //1.创建集合，用于存储返回结果
        ArrayList<T> resultList = new ArrayList<>();

        //2.预编译SQL
        PreparedStatement preparedStatement = conn.prepareStatement(querySQL);

        //3.执行查询语句
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //4.遍历ResultSet数据，将每行数据封装泛型并加入到结果集合中
        while (resultSet.next()) {
            //创建泛型对象
            T t = clazz.newInstance();

            //给泛型对象赋值
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (underScoreToCamel) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                BeanUtils.setProperty(t, columnName, resultSet.getObject(i));
            }
            //将对象加入集合
            resultList.add(t);
        }
        //关闭资源
        resultSet.close();
        preparedStatement.close();

        //返回查询结果
        return resultList;
    }

    //测试jdbc工具
    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        List<JSONObject> list = queryList(connection, "select * from GMALL_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true);
        long end = System.currentTimeMillis();
        List<JSONObject> list1 = queryList(connection, "select * from GMALL_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true);
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);  //195 211 188
        System.out.println(end2 - end);   //13  15
        System.out.println(list);
        System.out.println(list1);
    }
}
