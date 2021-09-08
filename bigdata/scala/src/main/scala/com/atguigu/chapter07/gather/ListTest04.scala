package com.atguigu.chapter07.gather

import scala.collection.mutable

object ListTest04 {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1, 2, 3, 4)
    val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
    val wordList: List[String] = List("hello world", "hello atguigu", "hello scala")

    // （1）过滤
    // 遍历一个集合并从中获取满足指定条件的元素组成一个新的集合
    val ints: List[Int] = list.filter(_ % 2 == 0)
    println(ints)

    // （2）转化/映射（map）
    // 将集合中的每一个元素映射到某一个函数
    val ints1: List[Int] = list.map(x => x + 1)
    println(ints1)

    // （3）扁平化
    val flatten: List[Int] = nestedList.flatten
    println(flatten)

    // （4）扁平化+映射 注：flatMap相当于先进行map操作，在进行flatten操作
    // 集合中的每个元素的子元素映射到某个函数并返回新集合
    val list1: List[List[String]] = wordList.map((s: String) => {
      val strings: Array[String] = s.split(" ")
      strings.toList
    })
    val flatten1: List[String] = list1.flatten
    println(flatten1)

    val strings: List[String] = wordList.flatMap(_.split(" ")
    )
    println(strings)

    // （5）分组(group)
    // 按照指定的规则对集合的元素进行分组
    val stringToStrings: Map[String, List[String]] = strings.groupBy((s: String) => s)
    println(stringToStrings)

    val intToInts: Map[Int, List[Int]] = list.groupBy((x: Int) => x % 2)
    println(intToInts)

    // （6）简化（归约）
    val i: Int = list.reduce((x: Int, y: Int) => x - y)
    println(i)

    //实现加法运算
    val i1: Int = list.reduce((x: Int, y: Int) => (x + y) * 2)
    println(i1)

    //reduce的底层就是reduceLeft
    val i2: Int = list.reduceLeft((x: Int, y: Int) => x - y)
    println(i2)

    //reduceRight从又开始计算
    //((4 - 3) - 2) - 1 = -2
    val i3: Int = list.reduceRight((x: Int, y: Int) => x - y)
    println(i3)

    // （7）折叠
    //10 - 1 - 2 - 3 - 4
    val i4: Int = list.fold(10)((x: Int, y: Int) => x - y)
    println(i4)

    //(8)两个Map的数据合并
    val map1: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3)
    val map2: mutable.Map[String, Int] = mutable.Map("a" -> 4, "b" -> 5, "d" -> 6)

    val stringToInt: mutable.Map[String, Int] = map1.foldLeft(map2)((res: mutable.Map[String, Int], kv: (String, Int)) => {
      val key: String = kv._1
      val value: Int = kv._2
      res(key) = value + res.getOrElse(key, 0)
      res
    })
    println(stringToInt)
  }

}
