package com.atguigu.hbase.dml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDml {
    //1.通过饿汉式单例模式设计util类
    public static Connection conn = null;

    //2.初始化单例连接
    static {
        //1.获取配置连接
        Configuration conf = HBaseConfiguration.create();

        //2.给配置类添加配置
        conf.set("hbase.zookeeper.quorum", "hadoop105,hadoop106,hadoop107");

        //3.获取连接
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //打印连接
        System.out.println("conn = " + conn);
    }

    public static void closeConnect() throws IOException {
        if (conn != null) {
            conn.close();
        }
    }

    //插入数据
    public static void putCell(String nameSpace,String tableName,String rowKey,String family,String column,String value) throws IOException {
        //1.获取table
        Table table = conn.getTable(TableName.valueOf(nameSpace, tableName));

        //2.创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.添加属性
        Put put1 = put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

        //4.put数据
        table.put(put);

        //5.关闭资源
        conn.close();
    }

    //查询数据
    public static String getCell(String nameSpace,String tableName,String rowKey,String family,String column) throws IOException {
        //1.获取table
        Table table = conn.getTable(TableName.valueOf(nameSpace, tableName));

        //2.获取get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //3.添加get属性
        Get get1 = get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        //4.get数据
        //简单用法，会出现乱码
//        byte[] bytes = table.get(get).value();
//        String value = bytes.toString();

        //复杂用法
        Result result = table.get(get);

        //获取cells
        Cell[] cells = result.rawCells();
        String value = "";
        for (Cell cell : cells) {
            //输出每个value
            value += Bytes.toString(CellUtil.cloneValue(cell)) + "-";
        }

        //5.关闭资源
        table.close();
        return value;
    }

    //扫描数据
    public static List<String> scanRows(String nameSpace, String tableName, String startRow, String stopRow) throws IOException {
        //1.获取表
        Table table = conn.getTable(TableName.valueOf(nameSpace, tableName));

        //2.获取scan对象
        Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));

        //3.扫描数据
        ResultScanner scanner = table.getScanner(scan);

        //4.获取结果
        ArrayList<String> strings = new ArrayList<>();

        for (Result result : scanner) {
            //strings.add(Bytes.toString(result.value()));
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                strings.add(Bytes.toString(CellUtil.cloneRow(cell)) + "-" +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + "-" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "-" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }

        //5.关闭资源
        table.close();
        return strings;
    }

    //删除数据
    public static void deleteColum(String nameSpace, String tableName, String rowKey, String family, String column) throws IOException {
        //1.获取表格
        Table table = conn.getTable(TableName.valueOf(nameSpace, tableName));

        //2.获取delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //3.添加删除列信息
        //3.1删除单个版本
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        //3.2删除所有版本
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));

        //3.3删除列族
        delete.addFamily(Bytes.toBytes(family));

        //4.删除数据
        table.delete(delete);

        //5.关闭资源
        table.close();
    }

    public static void main(String[] args) throws IOException {
        //上传数据
        //putCell("bigdata","student","1002","info","name","zhangsan3");

        //查询数据
//        String cell = getCell("bigdata", "student", "1001", "info", "name");
//        System.out.println(cell);

        //扫描数据
        List<String> strings = scanRows("bigdata", "student", "1001", "2000");
        for (String string : strings) {
            System.out.println(string);
        }

        //删除数据
        //deleteColum("bigdata", "student", "1001", "info", "name");
    }
}
