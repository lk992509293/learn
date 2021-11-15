package com.atguigu.test

object BubbleSort {

  def swap(arr: Array[Int], i: Int, j: Int) = {
    if (arr(i) != arr(j)) {
      arr(i) ^= arr(j)
      arr(j) ^= arr(i)
      arr(i) ^= arr(j)
    }
  }

  def main(args: Array[String]): Unit = {
    val arr = Array(5,4,3,2,8,9,6,1,8,3,8)

    for (i <- 0 until  arr.length - 1) {
      for (j <- i + 1 until arr.length) {
        if (arr(j) < arr(i)) {
          swap(arr, i, j)
        }
      }
    }
    arr.foreach(a => print(a + " "))
  }
}
