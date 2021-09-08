package com.atguigu.spark_project.project01

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object category_method5 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //2.1读取原始数据日志
    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\spark_project\\user_visit_action.txt")

    //2.2封装样例类 将lineRDD变为actionRDD
    val actionRDD: RDD[UserVisitAction] = rdd.map(line => {
      val dataSplit: Array[String] = line.split("_")
      //将解析出来的数据封装到样例类中
      UserVisitAction(
        dataSplit(0),
        dataSplit(1),
        dataSplit(2),
        dataSplit(3),
        dataSplit(4),
        dataSplit(5),
        dataSplit(6),
        dataSplit(7),
        dataSplit(8),
        dataSplit(9),
        dataSplit(10),
        dataSplit(11),
        dataSplit(12)
      )
    })

    //转换数据结构，将actionRDD转换为CategoryCountInfo
    val infoRDD: RDD[(String, CategoryCountInfo)] = actionRDD.flatMap(action => {
      if (action.click_category_id != "-1") {
        List((action.click_category_id, CategoryCountInfo(action.click_category_id, 1, 0, 0)))
      } else if (action.order_category_ids != "null") {
        val arr: Array[String] = action.order_category_ids.split(",")
        arr.map(id => (id, CategoryCountInfo(id, 0, 1, 0)))
      } else if (action.pay_category_ids != "null") {
        val arr: Array[String] = action.pay_category_ids.split(",")
        arr.map(id => (id, CategoryCountInfo(id, 0, 0, 1)))
      } else {
        Nil
      }
    })

    //按照品类id聚合
    val reduceRDD: RDD[CategoryCountInfo] = infoRDD.reduceByKey((info1, info2) => {
      info1.clickCount += info2.clickCount
      info1.orderCount += info2.orderCount
      info1.payCount += info2.payCount
      info1
    }).map(_._2)

    //排序取top10
    reduceRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10).foreach(println)


    //3.关闭资源
    sc.stop()
  }
}



