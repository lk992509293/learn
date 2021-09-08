package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class QueryJSON {
    public static void main(String[] args) throws IOException {
        //1.创建es客户端连接器
        JestClientFactory factory = new JestClientFactory();

        //2.创建es连接地址
        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop105:9200").build();

        //3.设置客户端连接配置
        factory.setHttpClientConfig(config);

        //4.获取es客户端连接
        JestClient jestClient = factory.getObject();

        //5.构建查询数据
        Search index = new Search.Builder("{\n" +
                "   \"query\": {\n" +
                "    \"bool\":{\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"男\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"橄榄球\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupByClass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\",\n" +
                "        \"size\": 10\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"groupByMaxAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 2\n" +
                " \n" +
                "}"
        )
                .addIndex("student")
                .addType("_doc")
                .build();

        //4.执行查询任务
        SearchResult result = jestClient.execute(index);

        //7.获取命中条数
        System.out.println("命中条数：" + result.getTotal());

        //8.获取数据详情
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            //遍历map集合
            for (Object o : source.keySet()) {
                System.out.println(o + ":" + source.get(o));
            }
        }
        //9.获取嵌套聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:" + bucket.getKey());
            System.out.println("doc_count:" + bucket.getCount());

            MaxAggregation maxAge = bucket.getMaxAggregation("maxAge");
            System.out.println("年龄最大值:" + maxAge.getMax());
        }


        //5.关闭连接
        jestClient.shutdownClient();

    }
}
