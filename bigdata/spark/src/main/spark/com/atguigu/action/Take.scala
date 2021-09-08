package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Take {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    println(rdd.take(3).mkString(", "))

    sc.stop()
  }
}
