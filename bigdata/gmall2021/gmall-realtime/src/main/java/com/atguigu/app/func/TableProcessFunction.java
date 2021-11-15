package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //声明Phoenix连接
    private Connection connection;

    //声明状态描述器属性
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //声明侧输出流标记
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction() {
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //1.获取数据并解析
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.创建Phoenix表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.将数据写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

    }


    //Phoenix建表操作 "create table if not exists db.tn(id varchar primary key,name varchar,...) sinkExtend"
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            //给主键以及扩展字段赋默认值
            if (sinkPk == null) {
                sinkPk = "id";
            }

            if (sinkExtend == null) {
                sinkExtend = "";
            }

            //构建建表语句
            StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            //拼接建表字段
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {

                String column = columns[i];

                //判断当前字段是否为主键
                if (sinkPk.equals(column)) {
                    createTableSQL.append(column).append(" varchar primary key");
                } else {
                    createTableSQL.append(column).append(" varchar");
                }

                //如果不是最后一个字段,则拼接","
                if (i < columns.length - 1) {
                    createTableSQL.append(",");
                }
            }

            createTableSQL.append(")").append(sinkExtend);

            //打印建表
            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            //执行操作
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException(sinkTable + "建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //1.读取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("table") + "-" + value.getString("op-type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            //2.过滤字段
            JSONObject data = value.getJSONObject("after");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(data, sinkColumns);

            //3.分流

            //补充SinkTable
            value.put("sinkTable", tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {

                //为Kafka数据,将数据写入主流
                out.collect(value);

            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {

                //为HBase数据,将数据写入侧输出流
                ctx.output(outputTag, value);
            }

        } else {
            System.out.println(key + "不存在！");
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columnsArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnsArr);

//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }
}
