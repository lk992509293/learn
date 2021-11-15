package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //1.获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.把数据源定义为动态表，从kafka读取数据
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("create table order_wide ( " +
                " province_id bigint, " +
                " province_name string, " +
                " province_area_code string, " +
                " province_iso_code string, " +
                " province_3166_2_code string, " +
                " order_id bigint, " +
                " split_total_amount decimal, " +
                " create_time string, " +
                " rt as TO_TIMESTAMP(create_time), " +
                " WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ") with ( " +
                MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) +
                ")");

        //执行查询语句，聚合计算
        Table table = tableEnv.sqlQuery("select " +
                "  DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "  DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "  province_id, " +
                "  province_name, " +
                "  province_iso_code, " +
                "  province_area_code, " +
                "  province_3166_2_code, " +
                "  sum(split_total_amount) order_amount, " +
                "  count(distinct order_id) order_count, " +
                "  UNIX_TIMESTAMP() as ts " +
                "from order_wide " +
                "group by province_id, " +
                "  province_name, " +
                "  province_iso_code, " +
                "  province_area_code,province_3166_2_code, " +
                "  TUMBLE(rt,INTERVAL '10' SECOND)");

        //将表数据转为流式数据
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);
        //打印一下
        provinceStatsDataStream.print();

        //将流式数据写入clickhouse
//        provinceStatsDataStream.addSink(ClickHouseUtil.getClickHouse("insert into province_stats " +
//                "values(?,?,?,?,?,?,?,?,?,?)"));

        //开启任务执行
        env.execute();
    }
}
