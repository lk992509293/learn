package com.atguigu.maven;

public class OurFriend {
    public String getMyName(String name) {
        Hello hello = new Hello();
        String str = hello.sayHello(name);
        return "This is OurFriend! " + str;
    }
}
