package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val list = List(("a",1),("a",3),("a",5),("b",7),("b",2),("b",4),("b",6),("a",7))

    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    rdd.foldByKey(0)((v1, v2) => v1 + v2).collect().foreach(println)

    sc.stop()
  }
}
