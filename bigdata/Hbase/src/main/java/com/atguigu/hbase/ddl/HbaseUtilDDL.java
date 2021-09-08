package com.atguigu.hbase.ddl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtilDDL {
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

    //创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {
        //1.获取admin
        Admin admin = conn.getAdmin();

        //创建返回结果
        //boolean res = false;

        //2.创建descriptor
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        //3.创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
            //res = true;
        } catch (IOException e) {
            System.out.println("命名空间已经存在！");
        }

        //4.关闭资源
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //return res;
    }

    //判断表格是否存在
    public static boolean isTableExist(String nameSpace, String tableName) throws IOException {
        //1.获取admin
        Admin admin = conn.getAdmin();

        //2.判断表是否存在
        boolean tableExists = admin.tableExists(TableName.valueOf(nameSpace, tableName));

        //3.关闭资源
        admin.close();

        return tableExists;
    }

    //创建表
    public static void createTable(String nameSpace, String tableName, String... familyNames) throws IOException {
        //1.判断是否存在列族信息
        if (familyNames.length <= 0) {
            System.out.println("没有设置列族信息！");
            return;
        }

        //2.判断表格是否存在
        if (isTableExist(nameSpace, tableName)) {
            System.out.println("表格已存在！");
            return;
        }

        //3.获取admin
        Admin admin = conn.getAdmin();

        //4.创建descriptor的builder
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace,tableName));

        //5.添加列族
        for (String familyName : familyNames) {
            //获取单个列族
            ColumnFamilyDescriptorBuilder columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));

            //添加版本
            columnFamily.setMaxVersions(2);

            //将当个列族的descriptor添加到builder中
            tableDescriptorBuilder.setColumnFamily(columnFamily.build());
        }

        //6.创建表
        admin.createTable(tableDescriptorBuilder.build());

        //7.关闭资源
        admin.close();

    }

    //修改表
    public static void modifyTable(String nameSpace, String tableName, String familyName, int version) throws IOException {
        //获取表是否存在
        if (!isTableExist(nameSpace, tableName)) {
            System.out.println("表格不存在！");
            return;
        }

        //1.获取admin
        Admin admin = conn.getAdmin();

        //2.获取原来表格的描述
        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(nameSpace, tableName));

        //3.获取原先描述的builder
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

        //4.在原先的builder中修改表格
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName)).setMaxVersions(version);

        //5.传递列族描述
        TableDescriptorBuilder tableDescriptorBuilder1 = tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());

        //6.传递表格描述
        admin.modifyTable(tableDescriptorBuilder.build());

        //7.关闭资源
        admin.close();
    }

    //删除表
    public static void deleteTable(String nameSpace, String table) throws IOException {
        //1.先检查表是否存在
        if (!isTableExist(nameSpace, table)) {
            System.out.println("表格不存在！");
            return;
        }

        //2.获取admin
        Admin admin = conn.getAdmin();

        //3.标记表格为disable
        admin.disableTable(TableName.valueOf(nameSpace, table));

        //4.删除表格
        admin.deleteTable(TableName.valueOf(nameSpace, table));

        //5.关闭资源
        admin.close();
    }

    public static void main(String[] args) throws IOException {
        //创建命名空间bigdata
        //createNameSpace("bigdata");

        //判断表是否存在
//        boolean tableExist = isTableExist("bigdata", "student");
//        if (!tableExist) {
//            System.out.println("该表不存在！");
//        } else {
//            System.out.println("该表存在！");
//        }

        //创建表格
        //createTable("bigdata", "student", "info");
//
//        //删除表格
//        //deleteTable("bigdata", "student");
//
//        //修改表格
        modifyTable("bigdata", "student","info", 5);

        //关闭连接
        closeConnect();
    }
}
