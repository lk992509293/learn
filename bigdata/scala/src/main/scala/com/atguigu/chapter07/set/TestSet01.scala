package com.atguigu.chapter07.set

object TestSet01 {
  def main(args: Array[String]): Unit = {
    //（1）Set默认是不可变集合，数据无序
    val set: Set[Int] = Set(1, 2, 3, 4)
    //（2）数据不可重复,并且是无序的
    val set1: Set[Int] = Set(1, 2, 3, 4, 5, 6, 3)
    //（3）遍历集合
    for (elem <- set1) {
      print(elem + " ")
    }
    println("\n======================")

  }
}
