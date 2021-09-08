package com.atguigu.twoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Intersection {
  def main(args: Array[String]): Unit = {
    //创建spark配置文件
    val conf: SparkConf = new SparkConf().setAppName("Intersection").setMaster("local[*]")

    //创建spark
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 4, 4)
    val value1: RDD[Int] = sc.makeRDD(3 to 7, 2)

    value.intersection(value1).collect.foreach(println)

    sc.stop()
  }
}
