package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.设置状态后端和checkpoint
//        env.setStateBackend(new FsStateBackend("hdfs:hadoop105:8020/flinkCDC/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(2000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 4.用DDL的方式从kafka读取数据
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

        tableEnv.executeSql("" +
                "CREATE TABLE page_view ( " +
                "  `common` MAP<STRING,STRING>, " +
                "  `page` MAP<STRING,STRING>, " +
                "  `ts` BIGINT, " +
                "  `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ") WITH ( " +
                MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) +
                ")");

        //TODO 5.过滤数据，过滤出"item_type":"keyword"  "item":"苹果手机"
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] full_word, " +
                "    rt " +
                "from page_view " +
                "where  " +
                "    page['item_type'] = 'keyword' " +
                "and " +
                "    page['item'] is not null");

        //创建临时视图
        tableEnv.createTemporaryView("filter_table", filterTable);

        //TODO 6.注册UDTF函数，做分词处理
        tableEnv.createTemporarySystemFunction("SplitFunc", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word, " +
                "    rt " +
                "FROM filter_table,  " +
                "LATERAL TABLE(SplitFunc(full_word))");
        //创建表临时视图
        tableEnv.createTemporaryView("split_table", splitTable);

        //TODO 7.按照单词分组计算WordCount
        Table resultTable = tableEnv.sqlQuery(" " +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP() as ts " +
                "from split_table " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 8.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();

        //TODO 9.写出数据到clickhouse
//        keywordStatsDataStream.addSink(ClickHouseUtil.getClickHouse("insert into " +
//                "keyword_stats_app(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 10.开启任务执行
        env.execute();
    }
}
