package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class QueryTest {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端连接器
        JestClientFactory factory = new JestClientFactory();

        //2.创建连接地址
        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop105:9200").build();

        //3.设置ES客户端连接配置
        factory.setHttpClientConfig(config);

        //4.获取ES连接器客户端
        JestClient jestClient = factory.getObject();

        //5.编写查询语句
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //------------------------query-----------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("name", "小");

        //---------------------------must-----------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "球");
        boolQueryBuilder.must(matchQueryBuilder);
        //---------------------------aggs-----------------------------
        TermsAggregationBuilder groupByClass1 = AggregationBuilders.terms("groupByClass").field("class_id").size(10);

        MaxAggregationBuilder maxAge1 = AggregationBuilders.max("maxAge").field("age");

        sourceBuilder.aggregation(groupByClass1.subAggregation(maxAge1));
        //sourceBuilder.aggregation(maxAge1);

        sourceBuilder.from(0);
        sourceBuilder.size(2);
        //6.执行查询
        Search search = new Search.Builder(sourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();

        SearchResult result = jestClient.execute(search);
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
        //9.获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:" + bucket.getKey());
            System.out.println("doc_count:" + bucket.getCount());
            MaxAggregation maxAge = bucket.getMaxAggregation("maxAge");
            System.out.println("年龄最大值:" + maxAge.getMax());
        }

        //10.关闭连接
        jestClient.shutdownClient();

    }

}
