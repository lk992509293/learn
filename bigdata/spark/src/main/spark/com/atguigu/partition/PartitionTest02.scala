package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionTest02 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sc
    val sc = new SparkContext(conf)

    //3.数据处理
    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5), 3)

    value.saveAsTextFile("output")


    //4.关闭资源
    sc.stop()
  }
}
