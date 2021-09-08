package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-sql-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取数据
    val df: DataFrame = spark.read.json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json")

    //df.show()

    //使用sparksql读取
    df.createOrReplaceTempView("user")

    //读取数据
    val sqlDF: DataFrame = spark.sql("select * from user")

    //打印查询结果
    sqlDF.show()

    //3.关闭资源
    spark.stop()
  }
}
