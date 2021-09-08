package com.atguigu.wordCountTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordCoutn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"), 2)

    val unit: RDD[String] = rdd.flatMap(_.split(" "))

    //对单词的结果进行转换
    val value: RDD[(String, Int)] = unit.map(str => (str, 1))

    //对value进行统计
    val value1: RDD[(String, Int)] = value.reduceByKey(_ + _)

    value1.collect.foreach(println)

    sc.stop()
  }
}
