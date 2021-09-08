package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Map").setMaster("local[*]")

    //2.创建spark
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)

    //打印每个元素位于哪个分区
    val unit: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, it) => it.map((index, _)))

    //打印
    unit.collect.foreach(println)

    //3.关闭资源
    sc.stop()
  }
}
