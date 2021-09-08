package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sample").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 2, 3, 3, 4, 5, 5, 6), 2)

    //对RDD数据去重
    rdd.distinct(3)
      .mapPartitionsWithIndex((index, it) => it.map((index, _))).collect.foreach(println)

    sc.stop()
  }
}
