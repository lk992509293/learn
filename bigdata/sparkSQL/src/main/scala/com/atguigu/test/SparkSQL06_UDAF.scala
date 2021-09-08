package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取数据
    val df: DataFrame = spark.read.json("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.json")

    //创建临时图
    df.createOrReplaceTempView("user")

    //注册UDAF
    spark.udf.register("myAvg", functions.udaf(new MyAvgUDAF()))

    //调用自定义的UDAF
    spark.sql("select myAvg(age) from user").show()

    //3.关闭资源
    spark.stop()
  }
}

case class Buffer(var sum: Long, var count: Long)

case class MyAvgUDAF() extends Aggregator[Long, Buffer, Double] {
  override def zero: Buffer = Buffer(0L, 0L)

  override def reduce(buffer: Buffer, age: Long): Buffer = {
    buffer.sum += age
    buffer.count += 1
    buffer
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer =  {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(buffer: Buffer): Double = {
    buffer.sum.toDouble / buffer.count
  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
