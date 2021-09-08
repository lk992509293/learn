package com.atguigu.chapter07.test02_single

class Person private{
  val name : String = "Person"
}

/*//懒汉式
object Person {
  private val person : Person = new Person

  //def getPerson: Person = person
  def apply(): Person = {
    println("apply")
    new Person()
  }
}*/

//饿汉式
object Person {
  private var person: Person = null

  def apply(): Person = {
    if (person == null) {
      synchronized(person)
      person = new Person
    }
    return person
  }
}

object Test_Singleton {
  def main(args: Array[String]): Unit = {
    //val person: Person = Person.getPerson
    //println(person.name)

    val person: Person = Person()
    println(person.name)
  }
}
