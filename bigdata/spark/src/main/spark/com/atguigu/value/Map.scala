package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Map {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val conf: SparkConf = new SparkConf().setAppName("Map").setMaster("local[*]")

    //2.创建spark
    val sc = new SparkContext(conf)

    //3.创建RDD
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    //4.将每个数*2输出
    val mapRDD: RDD[Int] = rdd.map(_*2)

    //5.手机并打印
    mapRDD.collect.foreach(println)

    //6.关闭资源
    sc.stop()
  }
}
