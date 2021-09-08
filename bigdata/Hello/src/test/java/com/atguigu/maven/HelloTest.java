package com.atguigu.maven;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class HelloTest {
    @Test
    public void testHello(){
        Hello hello = new Hello();
        String results = hello.sayHello("atguigu");
        //断言 判断结果和你预想的结果是否相同
        assertEquals("Hello atguigu!",results);
    }
}
