package com.atguigu.spark_project.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

object Category_method3 {
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
    reduceRDD.sortBy((_: (String, (Int, Int, Int)))._2, ascending = false).take(10).foreach(println)

    //3.关闭资源
    sc.stop()

  }
}
