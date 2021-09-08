package com.atguigu.chapter07.test01_trait

trait Ball {
  def describe: String = {
    "ball"
  }
}

trait Color extends Ball {
  override def describe : String = {
    "blue-" + super.describe
  }
}

trait Category extends Ball {
  override def describe: String = {
    "foot-" + super.describe
  }
}

class MyBall extends Category with Color {
  override def describe: String = {
    "my ball is a " + super.describe
  }
}

//特质叠加的问题
object TraitAdd {
  def main(args: Array[String]): Unit = {
    val ball: MyBall = new MyBall
    println(ball.describe)
  }
}
