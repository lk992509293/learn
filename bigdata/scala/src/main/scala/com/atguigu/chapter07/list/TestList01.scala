package com.atguigu.chapter07.list

object TestList01 {
  def main(args: Array[String]): Unit = {
    //（1）List默认为不可变集合
    //（2）创建一个List（数据有顺序，可重复）
    val list: List[Int] = List(1, 2, 3, 4, 3)
    //println(list.toString())

    //（3）遍历List
    for (elem <- list) {
      print(elem + " ")
    }
    println()
    //（4）List增加数据
    //把添加的数据放在list的开头
    val ints: List[Int] = 5 :: list
    for (elem <- ints) {
      print(elem + " ")
    }
    println()
    //把添加的数据放在list的开头
    val ints5: List[Int] = list.::(15)
    for (elem <- ints5) {
      print(elem + " ")
    }
    println()


    //在第一个位置添加元素
    //在list列表的后面添加元素
    val ints1: List[Int] = list :+ 5

    //在list列表的前面添加元素
    val ints2: List[Int] = 10 +: list

    for (elem <- ints1) {
      print(elem + " ")
    }

    println()

    //（5）集合间合并：将一个整体拆成一个一个的个体，称为扁平化
    val ints3: List[Int] = List(8, 9)
    val ints4: List[Int] = ints3 ::: list
    for (elem <- ints4) {
      print(elem + " ")
    }
    println()
    //（6）取指定数据
    println(list(1))
    //（7）空集合Nil
    val list4 = 1 :: 2 :: 3 :: 4 :: 5 :: Nil
    for (elem <- list4) {
      print(elem + " ")
    }
  }
}
