package com.atguigu.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object broadcast01 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //创建一个字符串RDD，过滤出包含WARN的数据
    val rdd: RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"), 4)
    val str: String = "WARN"

    //把str申明为广播变量
    val strBd: Broadcast[String] = sc.broadcast(str)

    val value: RDD[String] = rdd.filter(word => !word.contains(strBd.value))

    value.collect().foreach(println)

    //3.关闭资源
    sc.stop()
  }
}
