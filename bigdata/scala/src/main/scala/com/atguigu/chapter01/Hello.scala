package com.atguigu.chapter01

object Hello {
  def main(args: Array[String]): Unit = {
    // （1）以字母或者下划线开头，后接字母、数字、下划线
    var hello: String = "ok"
    var hell012: String = "ok"
    //    var 1hello : String = "ok"   error

    //    var a-b : String = "ok"   error

    //    var x h : String  = "ok"    error不能有空格

    var h_4: String = "ok"
    var _ab: String = "ok"

    var name: String = "zhangsan"
    var age: Int = 18

    println(name + " " + age)

    var s =
    """
        |select
        |   name,
        |   age
        |from user
        |where name = "zhangsan"
        |""".stripMargin
    println(s)

    var s1 =
    s"""
        |select
        |   name,
        |   age
        |   from user
        |   where name=$name and age=${age + 2}
        |""".stripMargin

    var s2 = s"name=$name  age=$age"
    println(s2)
  }
}

