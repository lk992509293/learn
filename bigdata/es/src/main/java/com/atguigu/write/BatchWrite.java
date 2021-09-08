package com.atguigu.write;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class BatchWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端对象
        JestClientFactory factory = new JestClientFactory();

        //2.设置连接参数
        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop105:9200").build();
        factory.setHttpClientConfig(config);

        //3.获取客户端对象
        JestClient jestClient  = factory.getObject();

        Movie movie1 = new Movie("1005", "二十不惑");
        Movie movie2 = new Movie("1006", "三十而立");
        Movie movie3 = new Movie("1007", "寄生虫");
        Index index1 = new Index.Builder(movie1).id("1005").build();
        Index index2 = new Index.Builder(movie2).id("1006").build();
        Index index3 = new Index.Builder(movie3).id("1007").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie_test1")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .addAction(index3)
                .build();
        jestClient.execute(bulk);
        jestClient.shutdownClient();

    }
}
