package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface AsyncDimJoinFunction<T> {
    //获取维度表的id
    String getId(T input);

    //补充维度信息
    void join(T input, JSONObject jsonObject) throws ParseException, Exception;
}
