package com.atguigu.twoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Union {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("union").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 4, 2)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 7, 2)

    rdd1.union(rdd2).collect.foreach(println)

    sc.stop()
  }
}
