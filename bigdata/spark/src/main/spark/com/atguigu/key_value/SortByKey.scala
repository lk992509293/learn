package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")), 2)

    rdd.sortByKey(ascending = true).collect().foreach(println)

    sc.stop()
  }
}
