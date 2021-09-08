package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取json数据
    val df: DataFrame = spark.read.json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json")

    //创建临时视图
    df.createOrReplaceTempView("user")

    //注册UDAF
    spark.udf.register("myAVG", functions.udaf(new MyAvgUDAF))

    //求平均年龄
    spark.sql("select myAVG(age) from user").show()

    //3.关闭资源
    spark.stop()
  }
}

//输入数据类型
case class Buff(var sum: Long, var count: Double)

case class MyAvgUDAF() extends Aggregator[Long, Buff, Double] {
  override def zero: Buff = Buff(0, 0)

  override def reduce(buff: Buff, age: Long): Buff = {
    buff.sum += age
    buff.count += 1
    buff
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(buff: Buff): Double = {
    buff.sum.toDouble / buff.count
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
