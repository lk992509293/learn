package com.atguigu.flume2;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class TimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取事件头
        Map<String, String> headers = event.getHeaders();

        //获取事件
        String log = new String(event.getBody());

        JSONObject  parse = (JSONObject) JSONObject.parseObject(log);

        //获取时间戳关键字
        String ts = parse.getString("ts");

        //在headers中timestamp标记为时间
        headers.put("timestamp", ts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder {

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
