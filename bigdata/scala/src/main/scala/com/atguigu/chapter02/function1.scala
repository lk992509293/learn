package com.atguigu.chapter02

object function1 {
  def main(args: Array[String]): Unit = {
    //（1）return可以省略，Scala会使用函数体的最后一行代码作为返回值
    def f1(name: String): String = {
      s"$name, are you ok?"
    }

    def test01(name: String): String = {
      val value = s"$name, are you ok?" //返回类型是Unit，需要再写一行value才会返回String
      value
    }

    println(f1("张三"))

    //（2）如果函数体只有一行代码，可以省略花括号
    def f2(name: String): String = s"$name, are you ok?"

    println(f2("张三"))

    //（3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    def f3(name: String) = s"$name, are you ok?"

    println(f3("张三"))

    //（4）如果有return，则不能省略返回值类型，必须指定
    def f4(name: String, age: Int): String = {
      //var x : Int = age + 10
      return s"$name, $age + 10"
    }

    println(f4("zhangsan", 18))

    //（5）如果函数明确声明unit，那么即使函数体中使用return关键字也不起作用
    def f5(name: String): Unit = {
      return s"$name, are you ok?"
    }

    f5("zhangsan")

    //（6）Scala如果期望是无返回值类型，可以省略等号
    def f6(name: String) {
      println(s"$name, are you ok?")
    }

    f6("zhangsan")

    //（7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def f7() = "dalang7"

    println(f7())

    //（8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    def f8 = "大郎喝药了8"

    println(f8)
    //（9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
      def function : String => String = (name: String) => s"hi $name"

      def f9: String => Unit = (x: String) => {
        println("wusong")
      }

      def f10(f: String => Unit): Unit = {
        f("")
      }

      f10(f9)
      println(f10((x: String) => {
        println("wusong")
      }))
  }
}
