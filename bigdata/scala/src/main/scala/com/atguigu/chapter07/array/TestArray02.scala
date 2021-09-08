package com.atguigu.chapter07.array

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TestArray02 {
  def main(args: Array[String]): Unit = {
    //创建可变数组
    val buffer01: ArrayBuffer[Int] = ArrayBuffer[Int](1, 2, 3, 4, 5)

    //遍历数组
    for (elem <- buffer01) {
      print(elem + " ")
    }
    println()

    //追加元素
    buffer01.append(6, 7, 8, 9, 10)
    for (elem <- buffer01) {
      print(elem + " ")
    }
    println()

    //向指定位置插入元素
    buffer01.insert(0, 0)
    buffer01.insert(0, 7, 8)
    for (elem <- buffer01) {
      print(elem + " ")
    }
    println()

    //修改指定位置的元素值
    buffer01(10) = 10
    for (elem <- buffer01) {
      print(elem + " ")
    }

    println("=============================")
    //创建一个不可变数组
    val ints: Array[Int] = Array(1, 2, 3, 4, 5)

    //创建一个可变数组
    val ints1: ArrayBuffer[Int] = ArrayBuffer(6, 7, 8, 9, 10)
    println()

    //可变数组转为不可变数组
    val arr: Array[Int] = ints1.toArray
    for (elem <- arr) {
      print(elem + " ")
    }

    println()
    //不可变数组转为可变数组
    val buffer: mutable.Buffer[Int] = ints.toBuffer
    buffer.append(11)
    for (elem <- buffer) {
      print(elem + " ")
    }
  }
}
