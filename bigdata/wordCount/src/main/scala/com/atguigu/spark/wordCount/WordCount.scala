package com.atguigu.spark.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件
    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2.创建SparkContext,并设置Spark App提交的入口
    val sc = new SparkContext(conf)

    //3.指定sc读取的文件位置
    //val lineRDD: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\wordCount\\input")
    val lineRDD: RDD[String] = sc.textFile(args(0))

    //4.扁平化读取到的数据
    val wordRdd: RDD[String] = lineRDD.flatMap(_.split(" "))
    
    //5.处理获取到的数据结果
    val wordToOneRDD: RDD[(String, Int)] = wordRdd.map((_, 1))
    
    //6.计算得到数据结果
    val wordToSumRdd: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)
    wordToSumRdd.saveAsTextFile(args(1))


    //7.将统计到的结果打印到控制台
    //val tuples: Array[(String, Int)] = wordToSumRdd.collect()
    //tuples.foreach(println)


    //8.关闭资源
    sc.stop()
  }
}
