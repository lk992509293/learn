package com.atguigu.wordCountTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordCoutn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"), 2)

    val unit: RDD[String] = rdd.flatMap(_.split(" "))

    val unit1: RDD[(String, Iterable[String])] = unit.groupBy(str => str)

    val value: RDD[(String, Int)] = unit1.mapValues(datas => (datas.size))
    value.collect.foreach(println)

    sc.stop()
  }
}
