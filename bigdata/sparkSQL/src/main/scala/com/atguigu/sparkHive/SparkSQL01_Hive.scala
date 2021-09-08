package com.atguigu.sparkHive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL01_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //连接外部hive
    spark.sql("show databases").show()
    spark.sql("show tables").show()

    //3.关闭资源
    spark.stop()
  }
}
