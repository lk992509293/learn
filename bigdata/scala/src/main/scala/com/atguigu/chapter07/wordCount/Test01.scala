package com.atguigu.chapter07.wordCount

import scala.collection.mutable

object Test01 {
  def main(args: Array[String]): Unit = {
    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    val list: List[String] = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")

    //1.扁平化
    val strings: List[String] = list.flatMap((str : String) => str.split(" "))
    println(strings)

/*    //1.扁平化
    var str : String = ""
    for (elem <- list) {
      str += elem + " "
    }
    val list1: List[String] = str.split(" ").toList
    println(list1)*/

    //2.遍历strings得到kv结构
    val map: mutable.Map[String, Int] = mutable.Map()
    for (elem <- strings) {
      if (!map.contains(elem)) {
        map.put(elem, 1)
      } else {
        map.put(elem, map.getOrElse(elem, 0) + 1)
      }
    }
    //println(map)

    //3.对得到的map按照value值降序排序
    //map没有排序的方法，将map转化为list,然后再调用list的sortWith方法排序
    val tuples: List[(String, Int)] = map.toList.sortWith((left: (String, Int), right: (String, Int)) => left._2 > right._2)
    //化简
    val tuples01: List[(String, Int)] = map.toList.sortWith((left, right) => left._2 > right._2)

    //4.获取第三部得到的list中top3的单词
    println(tuples.take(3))

  }
}
