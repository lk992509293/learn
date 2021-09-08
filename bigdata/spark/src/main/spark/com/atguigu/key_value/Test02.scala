package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\input\\agent.log")

    //将原属数据结构转换为（省份-广告，点击次数）的k-v结构
    val rdd1: RDD[(String, Int)] = rdd.map(line => {
      val strings: Array[String] = line.split(" ")
      (strings(1) + "-" + strings(4), 1)
    })

    //按照key分组，计算每个省份一种广告总共被点击多少次
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)

    //将数据结构转换为（省份，（广告，点击次数））
    val rdd3: RDD[(String, (String, Int))] = rdd2.map({
      case (str, num) => {
        val strings: Array[String] = str.split("-")
        (strings(0), (strings(1), num))
      }
    })

    //根据省份id进行分组
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()

    //按照点击次数排序并取top3
    val unit: RDD[(String, List[(String, Int)])] = rdd4.mapValues(datas => {
      datas.toList.sortWith(_._2 > _._2).take(3)
    })

    unit.collect().foreach(println)

    sc.stop()
  }
}
