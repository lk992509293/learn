package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object cache01 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\inputWordCount\\1.txt")

    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    rdd1.map(word =>{
      println("*************************")
      (word, 1)
    } )

    println(rdd1.toDebugString)

    //调用缓存，默认是仅内存，使用persist可以更改缓存级别，cache的底层也是调用persist
    rdd1.cache()

    rdd1.collect().foreach(println)

    println("============================")

    println(rdd1.toDebugString)

    rdd1.collect().foreach(println)

    Thread.sleep(1000000)

    sc.stop()
  }
}
