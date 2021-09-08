package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Glom").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    val unit: RDD[Int] = rdd.glom().map(_.max)

    println(unit.collect.sum)

    sc.stop()
  }
}
