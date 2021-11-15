package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements AsyncDimJoinFunction<T>{
    //声明Phoenix(jdbc)连接
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    //声明表名
    private String tableName;

    public AsyncDimFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //获取java线程池
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception{
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    //去查询Phoenix，获取维度数据
                    String id = getId(input);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                    //补充维度信息
                    join(input, dimInfo);

                    //将补充完的维度信息数据放入流中传递到下游
                    resultFuture.complete(Collections.singletonList(input));
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception{
        System.out.println("TimeOut:" + input);
    }
}
