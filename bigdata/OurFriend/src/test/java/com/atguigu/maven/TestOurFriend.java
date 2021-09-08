package com.atguigu.maven;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class TestOurFriend {
    @Test
    public void testOurFriend() {
        OurFriend ourFriend = new OurFriend();
        String str = ourFriend.getMyName("OurFriend");
        System.out.println(str);

        //断言
        assertEquals(str, "This is OurFriend! Hello OurFriend!");
    }
}
