package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Accumulator01_system {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    //需求:统计a出现的所有次数 ("a",10)
//    val value: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
//
//    value.collect().foreach(println)

    //声明一个系统累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach({
      case (word, num) => {
        sum.add(num)
      }
    })

    println("a " + sum.value)

    //3.关闭资源
    sc.stop()
  }
}
