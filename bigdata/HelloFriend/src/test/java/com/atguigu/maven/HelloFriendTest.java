package com.atguigu.maven;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class HelloFriendTest {
    @Test
    public void testHelloFriend() {
        HelloFriend hf = new HelloFriend();
        String str = hf.sayHelloToFriend("Maven");
        assertEquals(str, "This is OurFriend! Hello Maven!");
    }
}
