package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_FliePartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FliePartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val textFileLine: RDD[String] = sc.textFile("input2", 3)

    textFileLine.saveAsTextFile("out")

    sc.stop()
  }
}
