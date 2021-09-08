package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("combineByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val list = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))

    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    val unit: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1),
      ((tuple: (Int, Int), v: Int) => (tuple._1 + v, tuple._2 + 1)),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    unit.collect().foreach(println)

    unit.map({
      case (key, value) =>
        (key, value._1 / value._2.toDouble)
    }).collect().foreach(println)

    sc.stop()
  }
}
