package com.atguigu.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.util.DimPhoenixUtil;
import com.atguigu.util.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

//创建异步交互的工具类
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T,T> implements AsyncDimJoinFunction<T>{
    //声明连接参数
    private Connection connection;

    //声明线程池对象
    private ThreadPoolExecutor threadPoolExecutor;

    //创建表名属性
    private String tableName;

    public AsyncDimFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //加载Phoenix驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //创建连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //创建线程池对象
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        //开启线程池执行任务
        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                try {
                    //1.查询维度数据
                    String id = getKey(input);
                    JSONObject dimInfo = DimPhoenixUtil.getDimInfo(connection, tableName, id);

                    //2.补充维度信息
                    join(input, dimInfo);

                    //3.将补充完维度信息的数据放入流中
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("timeout:" + input);
    }
}
