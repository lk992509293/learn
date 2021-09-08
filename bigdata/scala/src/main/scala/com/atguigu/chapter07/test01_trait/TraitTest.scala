package com.atguigu.chapter07.test01_trait

trait TraitTest01 {
  //声明属性
  val name :String = "TraitTest01"

  //声明属性
  def eat() : Unit = {

  }

  //抽象属性
  var age : Int

  //抽象方法
  def sayHi() : Unit
}

trait TraitTest02 {
  //声明属性
  val name :String = "TraitTest02"

  //声明属性
  def eat() : Unit = {

  }

  //抽象属性
  var age : Int

  //抽象方法
  def sayHi() : Unit
}

class Person extends TraitTest01 with TraitTest02 {
  override var age: Int = 18
  override val name : String = "student"

  override def eat(): Unit = {
    println("吃饭")
  }

  override def sayHi(): Unit = {
    println("hello person")
  }
}

object TestMain {
  def main(args: Array[String]): Unit = {
    val person: Person = new Person

    println(person.name)
  }
}
