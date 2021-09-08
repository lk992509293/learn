package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("repartition").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(5, 2, 4, 3, 1, 6), 2)

    val unit: RDD[Int] = rdd.sortBy(num => num, false)

    unit.collect.foreach(println)

    sc.stop()
  }
}
