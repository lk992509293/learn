package com.atguigu.func;

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    //声明Phoenix连接
    private Connection connection;

    //声明状态描述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //声明侧输出流
    private OutputTag<JSONObject> outputTag;

    //从外部传入广播状态描述器和侧输出配置
    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //加载Phoenix连接驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);

        //初始化Phoenix连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取广播配置信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess properties = broadcastState.get(key);

        if (properties != null) {
            //2.过滤掉配置信息中不包含的列名字段
            JSONObject after = value.getJSONObject("after");
            String sinkColumns = properties.getSinkColumns();
            filterColumn(after, sinkColumns);

            //3.向字段中补充输出到哪个表的信息
            value.put("sinkTable", properties.getSinkTable());

            //4.将主流和侧输出流分开，根据配置信息，将数据输出到kafka或者hbase
            String sinkType = properties.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //如果输出到kafka，则走主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //如果输出到hbase，则走侧输出流
                ctx.output(outputTag, value);
            }
        } else {
            System.out.println(key + "不存在这样的表！！！");
        }
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //1.获取广播状态数据
        JSONObject jsonObject = JSON.parseObject(value);

        //2.获取广播流配置信息
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //3.创建Phoenix表
        //首先要判定表是否存在，先根据sink_type字段判断输出类型是否是hbase
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            //创建表
            createTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //4.将配置信息写入状态广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSinkTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    //Phoenix建表操作 "create table if not exists db.tn(id varchar primary key,name varchar,...) sinkExtend"
    public void createTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;

        //如果主键字段为null，则赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }

        //如果扩展字段为null，则赋空字符串为默认值
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //构建建表语句
        StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        //遍历列字段
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            //判断当前字段是否为主键
            if (sinkPk.equals(column)) {
                createTableSQL.append(column).append(" varchar primary key");
            } else {
                createTableSQL.append(column).append(" varchar");
            }

            //如果不是最后一个字段，则拼接“，”
            if (i < columns.length - 1) {
                createTableSQL.append(",");
            }
        }
        createTableSQL.append(")").append(sinkExtend);

        //打印建表语句
        System.out.println("createTableSQL = " + createTableSQL);

        //预编译sql
        try {
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败");
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
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
