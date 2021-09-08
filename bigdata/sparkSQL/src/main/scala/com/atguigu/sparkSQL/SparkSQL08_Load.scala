package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL08_Load {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark-sql-test").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    spark.read.json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json").show()

    //采用通用方式读取json
    spark.read.format("json").load("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json").show()

    spark.stop()
  }
}
