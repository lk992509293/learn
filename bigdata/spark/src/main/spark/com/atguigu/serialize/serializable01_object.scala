package com.atguigu.serialize

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object serializable01_object {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //创建两个对象
    val user1 = new User()
    user1.name = "zhangsan"

    val user2 = new User()
    user2.name = "lisi"

    val rdd: RDD[String] = sc.makeRDD(List(user1.name, user2.name))
    val rdd1: RDD[Nothing] = sc.makeRDD(List())

    //rdd.foreach(println)

    //此段代码并没有执行,所以并没有对象需要传递到executor端执行
    rdd1.foreach(println)

    //此段代码也需要类序列化才不报错，因为scala自带闭包检查
    rdd1.foreach(user => println(user1.name + " love " + user2.name))

    sc.stop()
  }
}

case class User() {
  var name : String = _
}
