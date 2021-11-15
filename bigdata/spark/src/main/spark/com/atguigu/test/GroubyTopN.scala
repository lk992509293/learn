package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroubyTopN {
  def main(args: Array[String]): Unit = {
    //创建spark配置文件，并设置spark名称
    val conf: SparkConf = new SparkConf().setAppName("topN").setMaster("local[*]")

    //创建sc
    val sc = new SparkContext(conf)

    val input: RDD[String] = sc.makeRDD(Array("hello", "123456", "hello", "abcd", "hello", "abcd", "spark", "scala", "spark",
      "spark", "hello"))

    val sortRDD: RDD[(String, Int)] = input.map((_,1)).reduceByKey(_+_).sortBy(_._2,false)

    sortRDD.take(3).foreach(println)

    //关闭spark资源
    sc.stop()
  }
}
