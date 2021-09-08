package com.atguigu.chapter07.wordCount

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Test02 {
  def main(args: Array[String]): Unit = {
    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    val list: List[String] = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")
    //println(list)

    //1.扁平化
    val strings: List[String] = list.flatMap((str: String) => str.split(" "))
    //println(strings)

    //2.得到strings按照字符串进行分组,就是把相同的单词放在一起
    val stringToList: Map[String, List[String]] = strings.groupBy((str: String) => str)

    //3.对相同的单词进行计数,以元组的形式保存数据
    val stringToInt: Map[String, Int] = stringToList.map(tuple => (tuple._1, tuple._2.size))
    //println(stringToInt)

    //4.对第3步得到集合按照value进行排序
    val tuples: List[(String, Int)] = stringToInt.toList.sortWith((left, right) => left._2 > right._2)

    //5.取出其top3的单词
    println(tuples.take(3))


    println(list.flatMap(_.split(" "))
      .groupBy(s => s)
      .map(t => (t._1, t._2.size))
      .toList.sortWith(_._2 > _._2)
      .take(3))
  }
}
