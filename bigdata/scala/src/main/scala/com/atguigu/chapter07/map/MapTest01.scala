package com.atguigu.chapter07.map

object MapTest01 {
  def main(args: Array[String]): Unit = {
    //（1）创建不可变集合Map
    val map: Map[Int, String] = Map((1, "hello"), (2, "world"))
    //（2）循环打印
//    for (elem <- map) {
//      val key: Int = elem._1
//      val value: String = elem._2
//      println(key + "->" + value)
//    }
    println(map)
    //println(map(3))//如果获取的key不存在会产生空指针异常
//    if (map.contains(3)) {
//      println(map(3))
//    }
    println(map.getOrElse(3, "null"))
    map.foreach((kv) => {println(kv)})//匿名函数的打印方式
    //（3）访问数据
    //（4）如果key不存在，返回0

  }
}
