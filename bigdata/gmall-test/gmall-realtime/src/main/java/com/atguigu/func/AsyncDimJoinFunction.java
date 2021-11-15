package com.atguigu.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface AsyncDimJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject jsonObject) throws ParseException;

}
