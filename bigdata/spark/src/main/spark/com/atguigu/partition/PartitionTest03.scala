package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionTest03 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sc
    val sc = new SparkContext(conf)


    //3.设置输出
    val value: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\input", 2)
    value.saveAsTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\output")
9
    //4.关闭资源
    sc.stop()
  }
}
