package com.nandu

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02NanDu {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //读取数据
    val lineRDD: RDD[String] = sc.textFile("input/nandu.csv")

    //转换数据结构
    val mapRDD: RDD[(String, String)] = lineRDD.map(info => {
      val strings: Array[String] = info.split(",")
      (strings(1), strings(0))
    })

    //过滤
    val filterRDD: RDD[(String, String)] = mapRDD.filter(info => !"Date".equals(info._2))

    //倒排后分组
    val groupRDD: RDD[(String, Iterable[String])] = filterRDD.groupByKey()

    //取最小时间
    val map02RDD: RDD[(String, Int)] = groupRDD.map(info => (info._2.min, 1))

    val subRDD: RDD[(String, Int)] = filterRDD.map(_._2).subtract(map02RDD.map(_._1)).distinct().map(info => (info, 0))

    map02RDD.union(subRDD).reduceByKey(_ + _).sortByKey().foreach(println)

    //3.关闭资源
    sc.stop()
  }
}
