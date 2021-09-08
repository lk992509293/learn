package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("cogroup").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(4,6)))

    rdd1.cogroup(rdd2).collect().foreach(println)

    sc.stop()
  }
}
