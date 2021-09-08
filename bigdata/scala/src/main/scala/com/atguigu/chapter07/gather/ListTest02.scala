package com.atguigu.chapter07.gather

object ListTest02 {
  def main(args: Array[String]): Unit = {
    //创建一个集合
    val list: List[Int] = List(1, 2, 3, 4, 5)
    val list01: List[Int] = List(6, 7, 8, 9, 10)

    //（1）获取集合的头
    println(list.head)

    //（2）获取集合的尾（不是头的就是尾）
    println(list.tail)

    //（3）集合最后一个数据
    println(list.last)

    //（4）集合初始数据（不包含最后一个）
    println(list.init)

    //（5）反转
    println(list.reverse)

    //（6）取前（后）n个元素
    println(list.take(3))

    //（7）去掉前（后）n个元素
    println(list.drop(3))

    //（8）并集
    println(list.union(list01))

    //（9）交集
    println(list.intersect(list01))

    //（10）差集
    println(list.diff(list01))

    //（11）拉链
    println(list.zip(list01))

    //（12）滑窗
    println("==========================")
    list.sliding(2, 1).foreach(println)

  }
}
