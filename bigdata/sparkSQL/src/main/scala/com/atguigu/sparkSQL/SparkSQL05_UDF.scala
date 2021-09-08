package com.atguigu.sparkSQL

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL05_UDF {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取json数据
    val df: DataFrame = spark.read.json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json")

    //创建临时视图
    df.createTempView("user")

    //注册UDF
    spark.udf.register("addName", (x:String) => "name:" + x)

    //调用自定义函数
    spark.sql("select addName(name),age from user").show

    //3.关闭资源
    spark.stop()
  }
}
