package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Repartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("repartition").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //重新分区
    val unit: RDD[Int] = rdd.repartition(3)

    unit.mapPartitionsWithIndex((index, it) => it.map((index, _))).collect.foreach(println)

    sc.stop()
  }
}
