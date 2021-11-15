package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    /**最终需要的类型如下：
     * {
     * "database":"",
     * "tableName":"",
     * "before":{"id":"","name":""},
     * "after":{"id":"","name":""},
     * "type":""
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSON对象用于存放返回的数据结果
        JSONObject res = new JSONObject(true);

        //处理库名和表名
        String[] split = sourceRecord.topic().split("\\.");
        
        //获取库名
        String database = split[1];
        //获取表名
        String tableName = split[2];

        //处理数据
        Struct value = (Struct)sourceRecord.value();

        Struct beforeValue = value.getStruct("before");
        //创建一个json对象，用于保存before的数据
        JSONObject beforeJson = new JSONObject();
        if (null != beforeValue) {
            Schema schemaBefore = beforeValue.schema();
            for (Field field : schemaBefore.fields()) {
                beforeJson.put(field.name(), beforeValue.get(field));
            }
        }

        Struct afterValue = value.getStruct("after");
        //创建一个json对象，用于保存after的数据
        JSONObject afterJson = new JSONObject();
        if (null != afterValue) {
            Schema schemaAfter = afterValue.schema();
            for (Field field : schemaAfter.fields()) {
                afterJson.put(field.name(), afterValue.get(field));
            }
        }

        //处理操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        System.out.println(type);
        if ("create".equals(type)) {
            type = "insert";
        }

        //先把数据封装到输出json中
        res.put("database", database);
        res.put("table", tableName);
        res.put("before", beforeJson);
        res.put("after", afterJson);
        res.put("op-type", type);

        //将数据写到结果输出
        collector.collect(res.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
