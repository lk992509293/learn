package com.atguigu.chapter07.map

import scala.collection.mutable

//可变的map
object TestMap02 {
  def main(args: Array[String]): Unit = {
    //（1）创建可变集合
    val map: mutable.Map[Int, String] = mutable.Map((1, "aaa"), (2, "bbb"), (3, "ccc"), 4 -> "ddd", 5 -> "eee")
    //（2）打印集合
    //map.foreach((kv: (Int, String)) => {println(kv)})
    //（3）向集合增加数据
    map.+=(7 -> "ggg")
    map.put(6, "fff")
    //（4）删除数据
    map.remove(5)

//    map.foreach((kv: (Int, String)) => {
//      println(kv)
//    })
    //（5）修改数据
    map.put(1, "xxx")
    map.update(2, "mmm")
    println(map)
  }
}
