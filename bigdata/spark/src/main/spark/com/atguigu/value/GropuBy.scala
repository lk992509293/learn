package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GropuBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupBy").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    val value: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

    value.collect.foreach(println)

    sc.stop()
  }
}
