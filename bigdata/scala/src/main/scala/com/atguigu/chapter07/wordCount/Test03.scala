package com.atguigu.chapter07.wordCount

import scala.collection.mutable

object Test03 {
  def main(args: Array[String]): Unit = {
    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    val list: List[(String, Int)] = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    //1.直接把list中的每一列元素的key拿出来单独统计，
    val map: mutable.Map[String, Int] = mutable.Map()
    //遍历list集合，把key按照“ ”拆分
    for (elem <- list) {
      val strings: Array[String] = elem._1.split(" ")
      //遍历得到的数组，将对应的字符串放到map中
      for (str <- strings) {
        if (!map.contains(str)) {
          map.put(str, elem._2)
        } else {
          map.put(str, map.getOrElse(str, 0) + elem._2)
        }
      }
    }
    //println(map)

    //2.对map的value值进行降序排序
    val tuples: List[(String, Int)] = map.toList.sortWith((left: (String, Int), right: (String, Int)) => left._2 > left._2)

    //3.去tuples的top3
    println(tuples.take(3))
  }
}
