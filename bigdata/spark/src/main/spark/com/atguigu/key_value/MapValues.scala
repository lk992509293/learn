package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapValues").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")), 2)

    val unit: RDD[(Int, String)] = rdd.mapValues(_ + "|||")

    unit.collect().foreach(println)

    sc.stop()
  }
}
