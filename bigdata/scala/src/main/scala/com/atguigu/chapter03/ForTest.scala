package com.atguigu.chapter03

import scala.util.control.Breaks

object ForTest {
  def main(args: Array[String]): Unit = {
    test01()
    //test02()
  }

  //3.break中断
  def test03(): Unit = {
    Breaks.breakable {
      for (elem <- 1 to 10) {
        println(elem)
        if (elem == 5) {
          Breaks.break()
        }
      }
    }
  }

  //2.打印九层妖塔
  def test02(): Unit = {
    for (i <- 1 to 18 if i % 2 != 0) {
      println(" " * ((18 - i)/2) + "*" * i)
      //print("*" * i)
      //println()
    }
  }


  //1.打印9*9表
  def test01(): Unit = {
//    for (i <- 1 to 9) {
//      for (j <- 1 to i) {
//        print(i + "*" + j + "=" + i * j + '\t')
//      }
//      println()
//    }

    for (i <- 1 to 9; j <- 1 to i) {
      print(s"$i*$j=${i * j}\t")
      if (i == j) {
        println()
      }
    }
  }
}
