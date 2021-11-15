package com.nandu

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01NanDu {
  def main(args: Array[String]): Unit = {
    //1.创建spark运行环境
    val conf: SparkConf = new SparkConf().setAppName("spark-test").setMaster("local[*]")

    //2.创建sparkcontext
    val sc = new SparkContext(conf)

    //读取数据
    val lineRDD: RDD[String] = sc.textFile("input/nandu.csv")

    //转换数据格式
    val mapRDD: RDD[(String, (String, String))] = lineRDD.map(line => {
      val strings: Array[String] = line.split(",")
      (strings(1), (strings(2), strings(3)))
    })

    //过滤掉表头数据
    val filterRDD: RDD[(String, (String, String))] = mapRDD.filter(info => !"Name".equals(info._1))

    //按照学生姓名分组
    val groupByKeyRDD: RDD[(String, Iterable[(String, String)])] = filterRDD.groupByKey()

    //先转换数据格式，计算学生参加了几门考试
    val gradeRDD: RDD[(String, Int, Any)] = groupByKeyRDD.map(info => {
      val it: Iterable[(String, String)] = info._2
      val size: Int = it.size
      val arr = new Array[Int](size)
      var index = 0

      //对课程去重
      val hashSet = new util.HashSet[String]()

      for (elem <- it) {
        arr(index) = elem._2.toInt
        index += 1
        hashSet.add(elem._1)
      }

      val ints: Array[Int] = arr.sortWith(_ > _)
      if (hashSet.size() == 1) {
        (info._1, hashSet.size(), (ints(0)))
      } else {
        (info._1, hashSet.size(), (ints(0), ints(1)))
      }
    })

    //输出数据
    gradeRDD.coalesce(1).saveAsTextFile("out//grade.txt")

    //3.关闭执行环境
    sc.stop()
  }
}
