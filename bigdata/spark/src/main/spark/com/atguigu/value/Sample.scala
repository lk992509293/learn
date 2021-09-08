package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sample").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), 2)

    //抽取数据不放回
    val unit: RDD[Int] = rdd.sample(withReplacement = false, 0.2)

    val unit1: Unit = unit.collect.foreach(println)

    println("================================")

    //抽取数据放回
    val value: RDD[Int] = rdd.sample(withReplacement = true, 0.5)
    value.collect.foreach(println)

    sc.stop()
  }
}
