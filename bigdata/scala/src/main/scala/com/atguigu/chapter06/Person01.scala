package com.atguigu.chapter06

class Person01() { //如果主构造器没有参数，则小括号不能省略
  println("主构造器")

  var name: String = _
  var age: Int = _

  def this(age: Int) {
    this()
    this.age = age
    println("辅助构造器")
  }

  def this(name : String, age : Int) {
    this(age)
    this.name = name
  }
}

object Person01 {
  def main(args: Array[String]): Unit = {
    //    val unit = new Person("zhangsan", 18)
    //    println(unit.name + ":" + unit.age)

    val person = new Person01(18)
  }
}

