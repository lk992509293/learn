package com.atguigu.chapter07.gather

object ListTest01 {
  def main(args: Array[String]): Unit = {
    //创建一个集合
    val list: List[Int] = List(1, 2, 3, 4, 5)

    //（1）获取集合长度
    println(list.length)

    //（2）获取集合大小
    println(list.size)

    //（3）循环遍历
    //1.通过iterator迭代器遍历集合
//    val iterator: Iterator[Int] = list.iterator
//    for (elem <- iterator) {
//      print(elem + " ")
//    }

    //2.通过foreach遍历
    list.foreach(println)

    //（4）迭代器

    //（5）生成字符串
    println(list.toString())
    //（6）是否包含
    println(list.contains(2))

  }
}
