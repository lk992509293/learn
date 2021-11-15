package com.atguigu.test

import java.lang.{Integer, Math}

import scala.util.control.Breaks

object QuickSort {

  def quickSort(arr: Array[Int], left: Int, right: Int): Unit = {
    if (left <= right) {
      return
    }

    var less = left - 1
    var more = right
    var index = left
    swap(arr, left + (Math.random() * (right - left + 1)).toInt, right)
    while (index < more) {
      if (arr(index) < arr(right)) {
        less += 1
        swap(arr, index, less)
        index += 1
      } else if (arr(index) < arr(right)) {
        more -= 1
        swap(arr, index, less)
      } else {
        index += 1
      }
    }
    swap(arr, more, right)
    quickSort(arr, left, less)
    quickSort(arr, more + 1, right)
  }

  def main(args: Array[String]): Unit = {
    val arr = new Array[Int](50)
    val tmp = new Array[Int](50)

    for (i <- 0 until arr.length) {
      arr(i) = (Math.random() * arr.length).toInt
    }

    quickSort(arr, 0, arr.length - 1)

    tmp.copyToArray(arr)
    tmp.sortWith((_: Int) > (_: Int))

    for (i <- 0 until arr.length) {
      Breaks.breakable(
        if (tmp(i) != arr(i)) {
          println("排序错误！！！！")
          Breaks.break()
        }
      )
    }
    println("排序正确！！！！")
  }

  def swap(arr: Array[Int], i: Int, j:Int): Unit = {
    if (arr(i) != arr(j)) {
      arr(i) ^= arr(j)
      arr(j) ^= arr(i)
      arr(i) ^= arr(j)
    }
  }
}
