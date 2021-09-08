package com.atguigu.sparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL03_RDDToDataSet {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.txt")

    //切割数据
    val rdd: RDD[(String, Long)] = lineRDD.map(line => {
      val strings: Array[String] = line.split(",")
      (strings(0), strings(1).toLong)
    })

    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //声明隐式转换
    import spark.implicits._
    
    val ds: Dataset[(String, Long)] = rdd.toDS()
    ds.show()

    //使用样例类
    val userRDD: RDD[User] = rdd.map(t => {
      User(t._1, t._2)
    })
    

    val dsUser: Dataset[User] = userRDD.toDS()

    dsUser.show()

    println("==============================")

    //将DS转换为RDD
    val rdd2: RDD[(String, Long)] = ds.rdd
    val userRdd2: RDD[User] = dsUser.rdd

    rdd2.collect().foreach(println)
    userRdd2.collect().foreach(println)

    //3.关闭资源
    sc.stop()
  }
}
