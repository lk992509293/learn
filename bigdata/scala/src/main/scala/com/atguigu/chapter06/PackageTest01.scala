package com.atguigu.chapter06

package object com {
  val name : String = "com"
}

package com {

  object PackageTest01 {
    def main(args: Array[String]): Unit = {
      println(name)
    }
  }

}