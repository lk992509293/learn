package com.atguigu.chapter02

import scala.io.StdIn
import scala.util.control.Breaks
import scala.util.control.Breaks._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    Breaks.breakable(
      for (elem <- 1 to 10) {
        println(elem)
        if (elem == 5) {
          println("提前结束循环")
          Breaks.break()
        }
      }
    )

    println("正常结束循环")
  }
}
