package com.atguigu.chapter07.wordCount

import scala.collection.mutable

object Test07 {
  def main(args: Array[String]): Unit = {
    val tuples: List[(String, String)] = List(("A", "hello scala world scala"), ("A", "hello scala hello spark"), ("B", "hello scala hello spark"), ("C", "hello scala hello spark"))

    //1.按照ABC品类进行分组聚合
    val map: mutable.Map[String, String] = mutable.Map()
    for (elem <- tuples) {
      if (map.contains(elem._1)) {
        map.put(elem._1, map.getOrElse(elem._1, "") + " " + elem._2)
      } else {
        map.put(elem._1, elem._2)
      }
    }
    println(map)

    //2.扁平化并分组
    val stringToStrings: collection.Map[String, List[String]] = map.mapValues(str => str.split(" ").toList)
    val stringToMap: collection.Map[String, Map[String, List[String]]] = stringToStrings.mapValues(list => list.groupBy(s => s))
    println(stringToMap)

    //3.统计每个单词出现的次数
    val stringToStringToInt: collection.Map[String, Map[String, Int]] = stringToMap.mapValues(map => map.mapValues(list => list.size))
    println(stringToStringToInt)

    //4.对单词出现的次数排序并取出top3
    val stringToTuples: collection.Map[String, List[(String, Int)]] = stringToStringToInt.mapValues(map => map.toList.sortWith(_._2 > _._2).take(3))
    println(stringToTuples)

  }
}
