package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {
    }


    @Override
    public Event intercept(Event event) {
        //1.获取事件体
        byte[] body = event.getBody();

        //2.根据首字符区分是字母还是数字
        byte b = body[0];

        //3.获取map映射
        Map<String, String> headers = event.getHeaders();

        //4.判断首字符‘b’的类型
        if (b >= '0' && b <= '9') {
            headers.put("type", "number");
        } else if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')) {
            headers.put("type", "letter");
        }

        //5.将header放回到event中(event为引用数据类型，修改它的值，在方法外该也会改变，所以可以不用再设置回去)
        event.setHeaders(headers);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            //处理每一个事件
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    //创建一个静态内部类Builder
    public static class MyBuilder implements Builder {
        @Override
        public Interceptor build() {
            //返回拦截器
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
