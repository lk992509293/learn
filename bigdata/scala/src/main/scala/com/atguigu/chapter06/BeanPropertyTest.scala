package com.atguigu.chapter06

import scala.beans.BeanProperty

class Person {
  var name : String = "bobo" //定义属性
  var age :Int = _ //"_"表示给属性一个默认值

  //定义为标准模板类属性
  @BeanProperty var sex : String = "男"
}


object BeanPropertyTest {
  def main(args: Array[String]): Unit = {
    val person = new Person

    println(person.getSex)
  }
}
