package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//使用jdbc连接clickhouse
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getClickHouse(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //通过反射获取泛型参数的对象
                        Class<?> aClass = t.getClass();
                        Field[] fields = aClass.getDeclaredFields();

                        //字段回退标记
                        int flag = 0;

                        //遍历所有的字段
                        for (int i = 0; i < fields.length; i++) {
                            try {
                                //通过反射的方式获取值
                                Field field = fields[i];
                                //设置为可获取模式
                                field.setAccessible(true);
                                Object value = field.get(t);

                                //如果某一个字段与sql字段无法对应上，当前该字段就需要与sql下一个字段匹配，所以要在原参数位置不动
                                //获取注解内容
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    flag++;
                                }

                                //给预编译sql赋值
                                preparedStatement.setObject(i + 1 - flag, value);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());
    }
}
