package com.atguigu.spark_project.project02

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

object Category_method1 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\spark_project\\user_visit_action.txt")

    //直接将数据结构转换为(str,(点击数，下单数，支付数))
    val flatMapRDD: RDD[(String, (Int, Int, Int))] = rdd.flatMap(line => {
      val dataSplit: ArrayOps.ofRef[String] = line.split("_")
      if (dataSplit(6) != "-1") {
        List((dataSplit(6), (1, 0, 0)))
      } else if (dataSplit(8) != "null") {
        val strings: Array[String] = dataSplit(8).split(",")
        strings.map((_, (0, 1, 0)))
      } else if (dataSplit(10) != "null") {
        val strings: Array[String] = dataSplit(10).split(",")
        strings.map((_, (0, 0, 1)))
      } else {
        Nil
      }
    })

    //将数据聚合
    val reduceRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey({
      case (t1, t2) =>
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    //将聚合后的数据进行排序并取top10
    val result: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy((_: (String, (Int, Int, Int)))._2, ascending = false).take(10)


    //********************需求二**********************
    //获取top10热门品类
    val ids: Array[String] = result.map(_._1)

    //创建广播变量
    val bdIds: Broadcast[Array[String]] = sc.broadcast(ids)

    //过滤原始数据，保留top10热门点击的数据
    val filterActionRDD: RDD[String] = rdd.filter(line => {
      val datas: Array[String] = line.split("_")
      if (datas(6) != "-1") {
        bdIds.value.contains(datas(6))
      } else {
        false
      }
    })

    //转换数据结构
    val idAndSessionToOneRDD: RDD[(String, Int)] = filterActionRDD.map(action => {
      val datas: Array[String] = action.split("_")
      (datas(6) + "=" + datas(2), 1)
    })

    //按照品类-会话id分组聚合
    val idAndSessionSumRDD: RDD[(String, Int)] = idAndSessionToOneRDD.reduceByKey(_ + _)

    //再次变换结构，分开品类和会话
    val idToSessionAndSumRDD: RDD[(String, (String, Int))] = idAndSessionSumRDD.map({
      case (key, sum) => {
        val str: Array[String] = key.split("=")
        (str(0), (str(1), sum))
      }
    })

    //再按照key分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = idToSessionAndSumRDD.groupByKey()

    //按照value排序
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith(_._2 > _._2)
      }.take(10)
    )

    resRDD.collect().foreach(println)

    //3.关闭资源
    sc.stop()

  }
}
