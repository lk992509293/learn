package com.atguigu.chapter07.list

import scala.collection.mutable.ListBuffer

object TestList02 {
  def main(args: Array[String]): Unit = {
    val listBuffer: ListBuffer[Int] = ListBuffer(1, 2 , 3 , 4)

    listBuffer.append(5)

    for (elem <- listBuffer) {
      print(elem + " ")
    }
    println()

    //修改数据
    listBuffer.update(1, 10)
    println(listBuffer)

    //删除数据
    listBuffer.remove(3)
    println(listBuffer)
  }
}
