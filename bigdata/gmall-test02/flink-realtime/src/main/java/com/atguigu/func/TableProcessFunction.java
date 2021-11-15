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
    //声明连接属性
    private Connection connection;

    //声明状态描述器
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

    //处理主流的元素
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableNmae") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        //获取配置表信息
        if (tableProcess != null) {
            //过滤没有的列名字段
            JSONObject after = value.getJSONObject("after");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(after, sinkColumns);

            //补充输出流向哪个表字段
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkTable = tableProcess.getSinkTable();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkTable)) {
                //输出到kafka
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkTable)) {
                //输出到侧输出流
                ctx.output(outputTag, value);
            }
        } else {
            System.out.println("不存在！！！");
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

    //处理广播流
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //创建Phoenix表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            //如果输出类型时到hbase，就需要创建对应的Phoenix表
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //将数据信息写出到Phoenix
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    //建表
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;

        try {
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
            e.printStackTrace();
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
}
