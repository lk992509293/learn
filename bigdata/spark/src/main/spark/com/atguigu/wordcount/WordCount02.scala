package com.atguigu.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取文件，利用foldByKey或aggregateByKey算子实现WordCount功能
object WordCount02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount02").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\inputWordCount\\1.txt")

    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map(str => (str, 1))

    val rdd3: RDD[(String, Int)] = rdd2.foldByKey(0)(_ + _)

    rdd3.collect().foreach(println)

    sc.stop()
  }
}
