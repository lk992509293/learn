package com.atguigu.test

object Test01 {

  def test(n: Byte): Unit = {
    println("aaaaaaaaaaaaaaaa")
  }

  def test(n: Short): Unit = {
    println("aaaaaaaaaaaaaaaa")
  }
  def test(n: Int): Unit = {
    println("aaaaaaaaaaaaaaaa")
  }
  def test(n: Long): Unit = {
    println("aaaaaaaaaaaaaaaa")
  }

  def main(args: Array[String]): Unit = {
    val n = 23
    test(n)

  }
}
