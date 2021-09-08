package com.atguigu.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop105", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = keyedStream.sum(1);

        /*连接es*/
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop105", 9200));
        httpHosts.add(new HttpHost("hadoop106", 9200));
        httpHosts.add(new HttpHost("hadoop107", 9200));

        ElasticsearchSink.Builder<Tuple2<String, Integer>> sumStreamBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void process(Tuple2<String, Integer> element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                //指定索引名，类型，_doc
                IndexRequest indexRequest = new IndexRequest("flink-0426", "1001");

                //将要写入的数据转换为json字符串
                String jsonString = JSON.toJSONString(element);
                IndexRequest request = indexRequest.source(jsonString, XContentType.JSON);

                requestIndexer.add(request);
            }
        });

        //设置最大缓存数据为一条
        sumStreamBuilder.setBulkFlushMaxActions(1);

        //发送数据到es
        sumStream.addSink(sumStreamBuilder.build());

        env.execute();
    }
}
