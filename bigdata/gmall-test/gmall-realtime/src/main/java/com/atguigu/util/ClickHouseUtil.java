package com.atguigu.util;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    //声明连接属性
    private Connection connection;

    public static <T> SinkFunction<T> getClickHouse(String sql) {
        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //通过泛型获取字段对象
                        Class<?> aClass = t.getClass();
                        Field[] fields = aClass.getDeclaredFields();

                        int j = 0;

                        //遍历字段
                        for (int i = 0; i < fields.length; i++) {
                            try {
                                //通过反射方式获取值
                                Field field = fields[i];
                                field.setAccessible(true);

                                Object value = field.get(t);

                                //获取字段上的注解，如果字段不是一一对应的，通过注解就可以跳过一个字段
                                TransientSink transientSink = field.getAnnotation(TransientSink.class);
                                if (transientSink != null) {
                                    j = j + 1;
                                    continue;
                                }

                                //给预编译SQL每一个对应的字段赋值
                                preparedStatement.setObject(i + 1, value);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
