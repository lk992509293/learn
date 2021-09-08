package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//统计每一个省份广告被点击次数的top3
object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("top3").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\input\\agent.log")

    val value: RDD[(String, Int)] = rdd.map({
      line: String => {
        val strings: Array[String] = line.split(" ")
        (strings(1) + "-" + strings(4), 1)
      }
    })

    //求出每个数据被点击次数
    val value1: RDD[(String, Int)] = value.reduceByKey(_ + _)

    //对统计的数据进行结构转换
    val value2: RDD[(String, (String, Int))] = value1.map({
      case (str, num) => {
        val strings: Array[String] = str.split("-")
        (strings(0), (strings(1), num))
      }
    })

    val value3: RDD[(String, Iterable[(String, Int)])] = value2.groupByKey()

    val value4: RDD[(String, List[(String, Int)])] = value3.mapValues({
      data: Iterable[(String, Int)] => {
        data.toList.sortWith(
          (_._2 > _._2)
        ).take(3)
      }
    })

    value4.collect().foreach(println)

    sc.stop()
  }
}
