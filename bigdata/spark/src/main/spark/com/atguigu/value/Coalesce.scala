package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sample").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //分区合并
    val unit: RDD[Int] = rdd.coalesce(2, shuffle = true)

    unit.mapPartitionsWithIndex((index, it) => it.map((index, _))).collect.foreach(println)

    sc.stop()
  }
}
