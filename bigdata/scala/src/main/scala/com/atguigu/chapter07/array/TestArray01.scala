package com.atguigu.chapter07.array

object TestArray01 {
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = new Array[Int](10)
    println(arr.length)

    //数组赋值
    arr(0) = 1
    arr.update(1, 2)
    println(arr.mkString(" "))

    //遍历数组
    for (elem <- arr) {
      print(elem + " ")
    }
    println()
    //简化遍历的步骤
    //arr.foreach(println)

    //增加数组的元素
    val ints: Array[Int] = arr :+ 5
    for (elem <- ints) {
      print(elem + " ")
    }
    println()

    //使用Array()的方式定义数组
    val arr01: Array[Int] = Array(1, 2, 3, 4)

    for (elem <- arr01) {
      print(elem + " ")
    }
  }
}
