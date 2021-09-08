package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccumulatorTest01 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    // 需求：自定义累加器，统计RDD中首字母为“H”的单词以及出现的次数。
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hive", "Spark", "Spark","Hive", "Hadoop"))

    //1.创建累加器
    val myAccumulator = new MyAccumulator01

    //2.注册累加器
    sc.register(myAccumulator)

    //3.使用累加器
    rdd.foreach(word => myAccumulator.add(word))

    //4.获取累加器的结果
    println(myAccumulator.value)

    //3.关闭资源
    sc.stop()
  }
}

//1.继承AccumulatorV2
//2.设定输入输出泛型
//3.实现六个抽象方法
class MyAccumulator01 extends AccumulatorV2[String, mutable.Map[String, Long]] {
  //创建一个map保存最终的数据结果
  var map: mutable.Map[String, Long] = mutable.Map()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator01

  override def reset(): Unit = map.clear()

  override def add(word: String) = {
    if (word.startsWith("H")) {
      map(word) = map.getOrElse(word, 0L) + 1L
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]) = {
    //遍历map2，把该分区的每一个k-v都加入到map1中
//    for (map2 <- other.value) {
//      map(map2._1) = map.getOrElse(map2._1, 0) + map2._2
//    }

    other.value.foreach({
      case (word, count) => {
        map(word) = map.getOrElse(word, 0L) + count
      }
    })
  }

  override def value: mutable.Map[String, Long] = map
}
