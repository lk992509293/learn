package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    val unit: RDD[String] = sc.makeRDD(Array("11", "222", "333", "4444"), 2)

    val str1: String = unit.aggregate("0")((x: String, y: String) => {
      Math.max(x.length, y.length).toString
    }, (a: String, b: String) => a + b)
    println(str1)
    val str2: String = unit.aggregate("")((x: String, y: String) => {
      Math.min(x.length, y.length).toString
    }, (a: String, b: String) => a + b)
    println(str2)
  }
}
