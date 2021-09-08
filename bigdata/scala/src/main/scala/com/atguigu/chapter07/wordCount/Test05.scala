package com.atguigu.chapter07.wordCount

import scala.collection.mutable

object Test05 {
  def main(args: Array[String]): Unit = {
    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")

    //1.先对list执行扁平化操作
    //val strings: List[String] = stringList.flatMap((str : String) => str.split(" "))
    //化简
    val strings: List[String] = stringList.flatMap(_.split(" "))
    //println(strings)

    //2.使用分组函数将每个字符串统计到一个map中
    val stringToStrings: Map[String, List[String]] = strings.groupBy(str => str)

    //3.统计每一个字符串出现的次数
    val map: Map[String, Int] = stringToStrings.map((tuple: (String, List[String])) => (tuple._1, tuple._2.size))
    println(map)

    //4.排序
    val tuples: List[(String, Int)] = map.toList.sortWith((left, right) => left._2 > right._2)

    //5.去top3的数据
    println(tuples.take(3))

  }
}
