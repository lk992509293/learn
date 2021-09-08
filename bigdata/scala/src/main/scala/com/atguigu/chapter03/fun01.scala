package com.atguigu.chapter03

object fun01 {
  def main(args: Array[String]): Unit = {
    println(fun01(0, '0', ""))

    //需求一：定义一个函数，输入的参数有三个，分别是(int,char,String)
    //如果输入的参数是(0,'0',"")返回false，否则返回true
    def fun01(a1: Int, b1: Char, s1: String) = a1 != 0 || b1 != '0' || s1 != ""

    println("==============================")

    //fun02()
    //需求二：定义一个函数，(int)(char)(String)
    //如果输入的参数是(0)('0')("")返回false，否则返回true
    def f1(i : Int) = {
      def f2(c : Char) = {
        def f3(s : String) = i != 0 || c != '0' || s != ""
        f3 _
      }
      f2 _
    }

    def f5(i : Int) = (c : Char) => (s : String) => i != 0 || c != '0' || s != ""

    def f6(i : Int) : Char => String => Boolean = c => i != 0 || c != '0' || _ != ""

    def f4(i : Int) : Char => String => Boolean = c => s => i != 0 || c != '0' || s != ""

    println(f4(0)('0')(""))

    (x : Int) =>  {x + 1}
  }
}
