package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL09_Save {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark-sql-test").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //获取数据
    val df: DataFrame = spark.read.json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json")

    //保存数据
    //df.write.save("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\out")

    //指定保存数据的格式
    //df.write.format("json").save("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\out")

    //指定写数据的方式为追加写
    //df.write.mode("append").json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\out")

    //指定写数据的方式为忽略
    //df.write.mode("ignore").json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\out")

    //如果文件已经存在，则覆盖写
    df.write.mode("overwrite").json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\out")

    spark.stop()
  }
}
