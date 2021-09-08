package com.atguigu.chapter03

import scala.util.control.Breaks

object Review01 {
  def main(args: Array[String]): Unit = {
    //breakTest()

    //method01()

    //method02()

    method03()
  }

  def method03(): Unit = {
    //准备一个基准函数
    def function(age : Int) = println("age = " + age)

    //将函数赋值给一个变量
    val func01 = function _

    //准备另一个基准函数
    def function01(func : Int => Unit,age : Int, name : String) = func(age)
    function01(func01, 10, "张三")
  }




  def method02(): Unit = {
    def func01(name :String, age : Int) = "name:" + name + "\nage:" + age

    val func02 = func01 _
    println(func02("lihua", 56))

    val func03 : (String, Int) => String = func01
    println(func03("zhangsan", 20))
  }



  def method01(): Unit = {
    def fun01(str : String): Unit = {
      println(str)
    }
    //fun01("hello")

    //1.可变参数
    //test("lihua")
    def test( s : String* ): Unit = {
      println(s)
    }

    // 一般情况下，将有默认值的参数放置在参数列表的后面
    def test4( sex : String = "男", name : String ): Unit =      {
      println(s"$name, $sex")
    }
    test4(name = "lihua")

    def fun  {return "lianzhipeng "}
  }

  def breakTest(): Unit = {
    Breaks.breakable(
      for (i <- 1 to 10 by 2) {
        if (i > 6) {
          Breaks.break()
        }
        println("i = " + i)
      }
    )
  }
}
