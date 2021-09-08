package com.atguigu.twoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Subtract {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("subtract").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 4, 2)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 7, 2)

    rdd1.subtract(rdd2).collect.foreach(println)

    sc.stop()
  }
}
