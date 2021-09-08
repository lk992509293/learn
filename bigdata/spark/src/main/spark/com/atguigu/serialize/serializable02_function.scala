package com.atguigu.serialize

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object serializable02_function {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3.创建一个RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    //创建一个Search对象
    val search = new Search("hello")

    // Driver：算子以外的代码都是在Driver端执行
    // Executor：算子里面的代码都是在Executor端执行
    //3.2 函数传递，打印：ERROR Task not serializable
    search.getMatch1(rdd).collect().foreach(println)

    //3.3 属性传递，打印：ERROR Task not serializable
    search.getMatche2(rdd).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}

class Search(query: String) {
  def isMatch(s : String): Boolean = {
    s.contains(query)
  }

  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  // 属性序列化案例
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter((x: String) => x.contains(query))
  }

}
