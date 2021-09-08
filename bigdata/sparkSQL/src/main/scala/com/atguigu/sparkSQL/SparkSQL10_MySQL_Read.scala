package com.atguigu.sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL10_MySQL_Read {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark-sql-test").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop105:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()

    //创建视图
    df.createOrReplaceTempView("user")

    //查询数据
    spark.sql("select * from user").show(50, truncate = false)

    spark.stop()
  }
}
