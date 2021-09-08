package com.atguigu.chapter02

import java.util.Date

object function {
  def main(args: Array[String]): Unit = {
    //定义函数
    def f(arg: String): Unit = {
      println(arg)
    }

    //函数调用
    f("hello")

    def test(name: String, age: Int): String = {
      return s"姓名:${name}\n年龄:${age + 2}"
    }

    val john: String = test("john", 18)
    println(john)

    new Date()

    def test1() : Unit = {
      def test2(name : String) : Unit = {
        println("函数可以嵌套！")
      }

      test2("zhangsan")
    }
    test1()


  }
}
