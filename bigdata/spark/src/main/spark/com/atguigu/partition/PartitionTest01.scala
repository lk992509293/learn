package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionTest01 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(conf)

    //创建RDD数据
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3 , 4))

    //3.输出数据
    rdd.saveAsTextFile("output")

    //关闭资源
    sc.stop()

  }
}
