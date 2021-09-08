package com.atguigu.chapter07.tuple

object TestTuple01 {
  def main(args: Array[String]): Unit = {
    //创建一个元组，元组的元素个数最多只能有22个
    val tuple: (Int, String, Boolean) = (40, "tuple", true)

    //访问元组
    println(tuple._1)
    println(tuple._2)
    println(tuple._3)

    //通过索引访问元组
    println(tuple.productElement(0))

    //通过迭代器访问元组
    for (elem <- tuple.productIterator) {
      print(elem + " ")
    }
  }
}
