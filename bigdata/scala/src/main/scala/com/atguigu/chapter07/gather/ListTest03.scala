package com.atguigu.chapter07.gather

object ListTest03 {
  def main(args: Array[String]): Unit = {
    val ints: List[Int] = List(1, 5, -3, 4, 2, -7, 6)

    //（1）求和
    val sum: Int = ints.sum
    println("sum = " + sum)

    //（2）求乘积
    val product: Int = ints.product
    println("mut = " + product)

    //（3）最大值
    val max: Int = ints.max
    println("max = " + max)

    //（4）最小值
    val min: Int = ints.min
    println("min = " + min)

    //（5）排序
    val ints1: List[Int] = ints.sortBy((x: Int) => x).reverse
    println(ints1)

    val ints2: List[Int] = ints.sortWith((x : Int, y : Int) => x < y)
    println(ints2)

    val ints3: List[Int] = ints.sortWith((x : Int, y : Int) => x > y)
    println(ints3)

  }

}
