package com.atguigu.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//读取文件，利用combineByKey算子实现WordCount功能
object WordCount03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount02").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\inputWordCount")

    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map(str => (str, 1))
  }
}
