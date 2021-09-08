package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountByKey {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (2, "ccc"), (4, "ddd"),(1, "ccc"), (2, "aaa"), (3, "ddd"), (4, "bbb")), 8)

    val map: collection.Map[Int, Long] = rdd.countByKey()

    println(map)

    sc.stop()
  }
}
