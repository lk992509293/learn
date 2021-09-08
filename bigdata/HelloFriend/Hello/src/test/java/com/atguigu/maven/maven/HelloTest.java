package com.atguigu.maven.maven;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HelloTest {
    @Test
    public void testHello() {
        Hello hello = new Hello();
        String res = hello.sayHello("atguigu");

        //断言，判断是否与预期结果相同
        assertEquals("Hello atguigu!", res);
    }
}
