package com.atguigu.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取文件，利用groupByKey或groupBy算子实现WordCount功能。
object WordCount01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount01").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\inputWordCount")

    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Iterable[String])] = rdd1.groupBy(str => str)

    val rdd3: RDD[(String, Int)] = rdd2.mapValues(datas => datas.size)

    rdd3.collect().foreach(println)

    sc.stop()
  }
}
