package com.atguigu.wordCountTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordCoutn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"), 2)

    val unit: RDD[String] = rdd.flatMap(_.split(" "))

    //对单词的结果进行转换
    val value: RDD[(String, Int)] = unit.map(str => (str, 1))

    //将转换后的数据进行分组
    val value1: RDD[(String, Iterable[(String, Int)])] = value.groupBy(map => map._1)

    //统计每个单词出现的次数
    val value2: RDD[(String, Int)] = value1.map({
      case (str, list) => (str, list.size)
    })

    value2.collect.foreach(println)

    sc.stop()
  }
}
