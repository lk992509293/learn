package com.atguigu.value

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Map").setMaster("local[*]")

    //2.创建spark
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //使用mappartitios计算每个数*2输出
    val rddPartitios: RDD[Int] = rdd.mapPartitions(it => it.map((x) => x * 2))

    //打印
    rddPartitios.collect.foreach(println)

    //3.关闭资源
    sc.stop()
  }
}
