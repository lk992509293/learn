package com.atguigu.func;

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

//自定义flinkCDC反序列化器
public class MyStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSON对象用于存放结果数据
        JSONObject result = new JSONObject();

        //处理库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        //处理数据本身
        Struct value = (Struct) sourceRecord.value();

        Struct beforeValue = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (beforeValue != null) {
            Schema beforeSchema = beforeValue.schema();
            for (Field field : beforeSchema.fields()) {
                beforeJson.put(field.name(), beforeValue.get(field));
            }
        }

        Struct afterValue = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (afterValue != null) {
            Schema afterSchema = afterValue.schema();
            for (Field field : afterSchema.fields()) {
                afterJson.put(field.name(), afterValue.get(field));
            }
        }

        //处理操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //将数据写入结果中并输出
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
