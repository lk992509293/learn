package com.atguigu.chapter07.wordCount

import scala.collection.mutable
import scala.collection.mutable.ArrayOps

object Test06 {
  def main(args: Array[String]): Unit = {
    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    //列表list保存的必然是kv结构
    val tupleList = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    //1.扁平化
    val tuples: List[(String, Int)] = tupleList.flatMap(tuple => {
      val strings: mutable.ArrayOps.ofRef[String] = tuple._1.split(" ")
      strings.map(word => (word, tuple._2))
    })
    println(tuples)

    //2.分组
    val stringToTuples: Map[String, List[(String, Int)]] = tuples.groupBy(str => str._1)
    println(stringToTuples)

    //3.进行map合并
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(data => data.map(t => t._2).sum)
    println(stringToInt)

    //排序
    val tuples1: List[(String, Int)] = stringToInt.toList.sortWith((left, right) => left._2 > right._2)
    println(tuples1)

    //去top3
    println(tuples1.take(3))


    //一步写完
    val list = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    val tuples2: List[(String, Int)] = list.flatMap(tuple => {
      tuple._1.split(" ")
        .map(str => (str, tuple._2))
    })
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
      .toList.sortWith((left, right) => left._2 > right._2)
      .take(3)
    println(tuples2)
  }
}
