package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\inputWordCount\\1.txt")

    val result: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).map((_, 1)).groupByKey().mapValues(_.size)

    result.collect().foreach(println)

    sc.stop()
  }
}
