package com.atguigu.sparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf

object SparkSQL04_DataFrameToDataSet {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    
    //读取json数据
    val df: DataFrame = spark.read.json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json")

    //声明隐式转换
    import spark.implicits._

    //将DF转换为DS
    val ds: Dataset[User] = df.as[User]

    ds.show()

    println("============================")

    //DataSet转换为DataFrame
    val userDf: DataFrame = ds.toDF()
    userDf.show()

    //3.关闭资源
    spark.stop()
  }
}
