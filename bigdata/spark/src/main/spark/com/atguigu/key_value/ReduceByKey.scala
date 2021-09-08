package com.atguigu.key_value

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("partitionBy").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)), 2)

    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    rdd1.collect.foreach(println)

    sc.stop()
  }
}
