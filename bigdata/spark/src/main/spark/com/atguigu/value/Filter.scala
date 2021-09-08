package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("filter").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val unit: RDD[Int] = rdd.filter(_ % 2 == 0)

    unit.collect.foreach(println)

    sc.stop()
  }
}
