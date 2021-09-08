package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("partitionBy").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    val rdd1: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))

    rdd1.mapPartitionsWithIndex((index, it) => it.map((index, _))).collect.foreach(println)

    sc.stop()
  }
}
