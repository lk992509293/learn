package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccumulatorTest02 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    // 需求：自定义累加器，统计RDD中首字母为“H”的单词以及出现的次数。
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hive", "Spark", "Spark","Hive", "Hadoop"))

    //2.1创建累加器
    val acc = new MyAccumulator02

    //2.2注册累加器
    sc.register(acc)

    //2.3使用累加器
    rdd.foreach((word: String) => acc.add(word))

    //2.4获取累加器的结果
    println(acc.value)

    //3.关闭资源
    sc.stop()
  }
}

//1.继承AccumulatorV2
//2.定义输入输出泛型
//3.实现六个抽象方法
class MyAccumulator02 extends AccumulatorV2[String, mutable.Map[String, Long]] {
  private var map: mutable.Map[String, Long] = mutable.Map()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator02

  override def reset(): Unit = map.clear()

  override def add(word: String): Unit = {
    if (word.startsWith("H")) {
      map(word) = map.getOrElse(word, 0L) + 1
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
//    other.value.foreach({
//      case (word, num) =>
//        map(word) = map.getOrElse(word, 0L) + num
//    })

    //使用集合遍历的方法
    for (map2 <- other.value) {
      map(map2._1) = map.getOrElse(map2._1, 0L) + map2._2
    }
  }

  override def value: mutable.Map[String, Long] = map
}
