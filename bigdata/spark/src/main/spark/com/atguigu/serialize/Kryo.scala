package com.atguigu.serialize

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Kryo {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //替换默认的序列化机制
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //注册需要使用kryo序列化自定义类
    conf.registerKryoClasses(Array(classOf[Searche01]))

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val search = new Searche01("hello")

    val unit: RDD[String] = search.getMatchedRDD1(rdd)

    unit.collect().foreach(println)

    sc.stop()
  }
}

class Searche01(val query: String) extends Serializable{

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchedRDD1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  def getMatchedRDD2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(_.contains(this.query))
  }
}

