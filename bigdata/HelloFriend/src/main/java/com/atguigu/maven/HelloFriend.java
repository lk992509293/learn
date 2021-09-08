package com.atguigu.maven;


public class HelloFriend {
    public String sayHelloToFriend(String name) {
//        Hello hello = new Hello();
//        String str = hello.sayHello(name) + " I am " + this.getNmae();

        OurFriend ourFriend = new OurFriend();
        String str = ourFriend.getMyName(name);

        return str;
    }

    public String getNmae() {
        return "Idea";
    }
}
