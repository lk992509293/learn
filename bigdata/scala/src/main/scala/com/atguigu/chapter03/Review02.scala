package com.atguigu.chapter03

object Review02 {
  def main(args: Array[String]): Unit = {
    // （1）定义一个函数：参数包含数据和逻辑函数
    def operation(arr: Array[Int], op: Int => Int) = {
      for (elem <- arr) yield op(elem)
    }

    //2.定义逻辑函数
    def op(elem: Int): Int = {
      elem + 1
    }

    //3.标准函数使用
    val arr = operation(Array(1, 2, 3, 4), op)
    println(arr.mkString(","))

    //4.使用匿名函数
    val arr01 = operation(Array(1, 2, 3, 4), (elem: Int) => {
      elem + 1
    })
    println(arr01.mkString("-"))

    //4.1参数的类型可以省略,类型省略之后，发现只有一个参数，则圆括号可以省略；
    // 其他情况：没有参数和参数超过1的永远不能省略圆括号。
    val arr02 = operation(Array(1, 2, 3, 4), elem => {
      elem + 1
    })
    println(arr02.mkString("-"))

    //4.2匿名函数如果只有一行，则大括号也可以省略
    val arr03 = operation(Array(1, 2, 3, 4), elem => elem + 1)
    println(arr03.mkString("-"))

    //4.3如果参数只出现一次，则参数省略且后面参数可以用_代替
    val arr04 = operation(Array(1, 2, 3, 4), _ + 1)
    println(arr04.mkString("! "))
  }
}
