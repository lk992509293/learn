package com.atguigu.twoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Zip {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("zip").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //元素个数不同不能拉链，分区数不同不能拉链
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)
    val rdd2: RDD[String] = sc.makeRDD(Array("a", "b", "c"), 3)

    rdd1.zip(rdd2).collect.foreach(println)

    sc.stop()
  }
}
