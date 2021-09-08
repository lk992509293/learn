package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPoint {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //需要设置检查点路径，否则会抛异常
    sc.setCheckpointDir("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\ck")

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\inputWordCount\\1.txt")

    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Long)] = rdd1.map((word: String) => {
      (word, System.currentTimeMillis())
    })

    //增加一个cache，做检查点时直接从该缓存中拿数据，避免再跑一个job
    rdd2.cache()

    //针对rdd2做数据检查点
    rdd2.checkpoint()

    //触发逻辑，行动算子执行后会立即启动一个新的job来做checkpoint运算
    rdd2.collect().foreach(println)
    println("==================================")

    //3.3 再次触发执行逻辑
    rdd2.collect().foreach(println)
    println("==================================")
    rdd2.collect().foreach(println)
    println("==================================")

    Thread.sleep(10000000)


    //3.关闭资源
    sc.stop()
  }
}
