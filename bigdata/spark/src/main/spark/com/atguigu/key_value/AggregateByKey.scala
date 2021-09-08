package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 5), ("b", 7), ("b", 2), ("b", 4), ("b", 6), ("a", 7)), 2)

    val value: RDD[(String, Int)] = rdd.aggregateByKey(0, 2)(math.max(_, _), _ + _)

    value.collect().foreach(println)

    sc.stop()
  }
}
