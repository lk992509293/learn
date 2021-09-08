package com.atguigu.chapter03

object Preview {
  def main(args: Array[String]): Unit = {
/*    def f = () => {
      println("...")
      10
    }
    foo(f())

    def foo(a: =>Int) : Unit = {
      println(a)
      println(a)
    }*/

    //准备一个基本函数1
    def functions (i : Int, name : String) :Unit = println("age=" + i + ":" + name )
    //将函数赋值给一个参数
    val func = functions _
    //准备另外一个函数2
    def functions1 (f : (Int, String) => Unit , name : String, age : Int) = f(age, name)
    //调用函数2，并将函数1传入
    functions1(func,"lianzhipeng", 20)
  }
}
