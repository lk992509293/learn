package com.atguigu.chapter07.set

import scala.collection.mutable

object TestSet02 {
  def main(args: Array[String]): Unit = {
    //（1）创建可变集合mutable.Set
    val set: mutable.Set[Int] = mutable.Set(1, 2, 3, 4, 5, 6)

    //（2）打印集合
    //set.foreach(println)
    println(set)

    //（3）集合添加元素
    set.add(10)
    set.add(8)
    println(set)

    //（4）向集合中添加元素，返回一个新的Set
    val ints: mutable.Set[Int] = set.+(11)
    println(ints)
    //（5）删除数据
    set.remove(5)
    println(set)

    //打印集合
    println(set.mkString(","))
  }
}
