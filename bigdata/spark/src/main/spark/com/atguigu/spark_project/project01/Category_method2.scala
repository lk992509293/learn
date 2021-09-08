package com.atguigu.spark_project.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayOps

object Category_method2 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //1.读取文件资源
    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\spark_project\\user_visit_action.txt")

    //2.过滤无效点击数据
    val clickFilterRDD: RDD[String] = rdd.filter(line => {
      val datas: Array[String] = line.split("_")
      datas(6) != "-1"
    })

    //3.转换数据格式并进行聚合，求出每个商品id被点击次数
    val clickCountRDD: RDD[(String, Int)] = clickFilterRDD.map(datas => {
      val datasSplit: Array[String] = datas.split("_")
      (datasSplit(6), 1)
    }).reduceByKey(_ + _)

    //4.过滤无效下单数据
    val orderFilterRDD: RDD[String] = rdd.filter(line => {
      val strSplit: Array[String] = line.split("_")
      strSplit(8) != "null"
    })

    //5.转换数据格式并进行聚合，求出每个商品id被下单次数
    val orderCountRDD: RDD[(String, Int)] = orderFilterRDD.flatMap((datas: String) => {
      val datasSplit: mutable.ArrayOps.ofRef[String] = datas.split("_")
      val cid: mutable.ArrayOps.ofRef[String] = datasSplit(8).split(",")
      cid.map((_, 1))
    }).reduceByKey(_ + _)

    //6.过滤无效支付数据
    val payFilterRDD: RDD[String] = rdd.filter(line => {
      val datas: Array[String] = line.split("_")
      datas(10) != "null"
    })

    //7.转换数据格式并进行聚合，求出每个商品id被支付次数
    val payCountMapRDD: RDD[(String, Int)] = payFilterRDD.flatMap(line => {
      val datas: ArrayOps.ofRef[String] = line.split("_")
      val ds: ArrayOps.ofRef[String] = datas(10).split(",")
      ds.map((_, 1))
    }).reduceByKey(_ + _)

    //可以使用cogroup进行全连接，但是会走shuffle，导致效率比较低
    //（String，（int，int，int））
    val clickRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.map({
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    })

    val orderRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map({
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    })

    val payRDD: RDD[(String, (Int, Int, Int))] = payCountMapRDD.map({
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    })

    //使用union并集来连接
    val unionRDD: RDD[(String, (Int, Int, Int))] = clickRDD.union(orderRDD).union(payRDD)

    val unionRDD2: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey((t1: (Int, Int, Int), t2: (Int, Int, Int)) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    //倒序排序取前10
    unionRDD2.sortBy(_._2, false).take(10).foreach(println)

    //3.关闭资源
    sc.stop()
  }
}
