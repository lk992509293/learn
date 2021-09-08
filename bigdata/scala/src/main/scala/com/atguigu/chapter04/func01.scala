package com.atguigu.chapter04

object func01 {
  def main(args: Array[String]): Unit = {
    def sayHi(name : String) = {
      def func01(age : Int) = {
        println(s"${age}岁的${name}")
      }
      func01 _
    }

    val function: Int => Unit = sayHi("linhai")
    function(10)
  }
}
