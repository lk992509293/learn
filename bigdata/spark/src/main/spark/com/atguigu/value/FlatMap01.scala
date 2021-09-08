package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMap01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
//    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala", "hello wolrd"))
//
//    val value: RDD[String] = rdd.flatMap(_.split(" "))
//
//    value.collect.foreach(println)

    val value: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7)), 2)

    val value1: RDD[Any] = value.flatMap((datas: List[Int]) => {
      datas match {
        case list: List[_] => list
        case dat => List(dat)
      }
    })
    value1.collect.foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
