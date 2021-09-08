package com.atguigu.spark_project.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//需求1：Top10热门品类
// 先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
object Category_method1 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Category_method1").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //2.1读取原始数据日志
    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\spark_project\\user_visit_action.txt")

    //2.2 过滤掉不是点击的数据
    val clickActionRDD: RDD[String] = rdd.filter(line => {
      val strings: Array[String] = line.split("_")
      strings(6) != "-1"
    })

    //2.3 统计点击品类的数量（商品品类，点击数）
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map {
      line: String => {
        val datas: Array[String] = line.split("_")
        (datas(6), 1)
      }
    }.reduceByKey(_ + _)

    //2.4 过滤掉不是下单的数据
    val orderActionRDD: RDD[String] = rdd.filter((line: String) => {
      val strings: Array[String] = line.split("_")
      strings(8) != "null"
    })

    //2.5 统计下单品类的数量（商品品类，下单数）
    //要使用flatMap对元组炸裂，得到（id，下单数）的格式，map算子最终只会得到（数组，下单数）的k-v结构
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap {
      line: String => {
        val datas: Array[String] = line.split("_")
        val cids: Array[String] = datas(8).split(",")
        //此处map会把数组中的每一个元素映射为（id，1）结构，然后会把数组中的所有元素存储到tuples元组中，最终flatMap把tuples炸裂
        val tuples: mutable.ArraySeq[(String, Int)] = cids.map(cids => (cids, 1))
        tuples
      }
    }.reduceByKey(_ + _)

    //2.6 过滤掉不是支付的数据
    val payActionRDD: RDD[String] = rdd.filter((line: String) => {
      val strings: Array[String] = line.split("_")
      strings(10) != "null"
    })

    //2.7 统计支付品类的数量（商品品类，支付数）
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap {
      line: String => {
        val datas: Array[String] = line.split("_")
        val cids: Array[String] = datas(10).split(",")
        cids.map((_, 1))
      }
    }.reduceByKey(_ + _)

    //2.8把所有的数据合并起来，构建商品（id，（点击数，下单数，支付数））的k-v结构
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    //2.9 将数据结构转换为(String, (int, int, int))
    val valueRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues({
      case (iter1, iter2, iter3) =>
        (iter1.sum, iter2.sum, iter3.sum)
    })

    //2.10 按照点击数 -> 下单数 -> 支付数 排序，并取top10
    valueRDD.sortBy(_._2, false).take(10).foreach(println)


    //3.关闭资源
    sc.stop()
  }
}
