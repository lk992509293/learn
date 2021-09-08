package com.atguigu.chapter07.wordCount

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayOps

object Test04 {
  def main(args: Array[String]): Unit = {
    val tuples: List[(String, Int)] = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    //1.把列表list的每一个元组的第一个元素按照空格拆分
    val list: List[(String, Int)] = tuples.flatMap((tuple: (String, Int)) => {
      val strings: mutable.ArrayOps.ofRef[String] = tuple._1.split(" ")
      strings.map((word: String) => (word, tuple._2))
    })
    //println(list)

    val list01: List[(String, Int)] = tuples.flatMap(tuple => {
      val strings: mutable.ArrayOps.ofRef[String] = tuple._1.split(" ")
      strings.map((word: String) => (word, tuple._2))
    })

    //按照单词分组
    val stringToTuples: Map[String, List[(String, Int)]] = list.groupBy(str => str._1)
    //println(stringToTuples)

    //进行map合并
    val stringToInts: Map[String, Int] = stringToTuples.mapValues(datas => datas.map(t => t._2).sum)
    //println(stringToInts)

    //对value值进行排序
    val tuples1: List[(String, Int)] = stringToInts.toList.sortWith((left, right) => left._2 > right._2)

    //去top3的数据
    println(tuples1.take(3))
  }
}
