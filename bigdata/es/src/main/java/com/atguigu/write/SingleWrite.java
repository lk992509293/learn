package com.atguigu.write;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class SingleWrite {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端构建器
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop105:9200").build();

        //3.设置ES连接地址
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();

        //jestClient.shutdownClient();

        //5.构建ES插入数据对象
        Index index = new Index.Builder("{\n" +
                "  \"id\":\"1002\",\n" +
                "  \"movie_name\":\"姜子牙\"\n" +
                "}")
                .index("movie_test1")
                .type("_doc")
                .id("1002")
                .build();


        //6.执行插入数据操作
        jestClient.execute(index);

        //7.关闭连接
        jestClient.shutdownClient();

    }
}
