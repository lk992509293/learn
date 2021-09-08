package com.atguigu.chapter02

object function2 {
  def main(args: Array[String]): Unit = {
    //（1）调用foo函数，把返回值给变量f1
    def foo(): String = {
      return "foo"
    }

    def f1: String = foo

    println(f1)

    //（2）在被调用函数foo后面加上 _，相当于把函数foo当成一个整体，传递给变量f1
    def f2 = foo _

    println(f2)

    //2）函数可以作为参数进行传递
    def add(a: Int, b: Int): Int = a + b

    def f3(add: (Int, Int) => Int): Int = add(2, 3)

    println(f3(add))
  }
}
