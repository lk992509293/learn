package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop105", 11111), "example", "", "");

        while (true) {
            //2.获取连接
            canalConnector.connect();

            //3.指定要监控的数据库
            canalConnector.subscribe("gmall.*");

            //4.获取message
            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size() < 0) {
                System.out.println("没有数据，休息一会儿");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //获取表名
                    String tableName = entry.getHeader().getTableName();

                    //获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //判断entry是否是RowData
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //TODO 根据条件获取数据
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取订单表的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);

            //获取订单详情表的新增数据
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);

            //获取用户表的新增及变化数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }

    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //获取存放列的集合
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //获取每个列
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
