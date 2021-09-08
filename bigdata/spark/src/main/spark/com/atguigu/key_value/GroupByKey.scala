package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)), 2)

    val unit: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    unit.collect.foreach(println)

    unit.map(t => (t._1, t._2.sum)).collect().foreach(println)

    sc.stop()
  }
}
