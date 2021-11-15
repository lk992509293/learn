package com.atguigu.sparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL02_RDDToDataFrame {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")
    
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    
    val lineRDD: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\sparkSQL\\input\\user.txt")

    //将读取到的数据切割
    val rdd: RDD[(String, Long)] = lineRDD.map((line: String) => {
      val datas: Array[String] = line.split(",")
      (datas(0), datas(1).toLong)
    })

    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //声明隐式转换
    import spark.implicits._

    //普通rdd转换为DF需要手动补上列名
    val df: DataFrame = rdd.toDF("name", "age")

    //打印数据
    df.show()

    println("=====================================")

    //样例类的方式将rdd转换为DF
    val userRDD: RDD[User] = rdd.map(t => {
      User(t._1, t._2)
    })

    //样例类RDD转换为DF
    val userDF: DataFrame = userRDD.toDF()

    userDF.show()

    //DF转换为RDD，直接转换即可
    val rdd1: RDD[Row] = userDF.rdd
    rdd1.collect().foreach(println)

    //获取Row里面的数据，直接row.get(索引)即可
    val rdd2: RDD[(String, Long)] = rdd1.map(row => {
      (row.getString(0), row.getLong(1))
    })

    rdd2.collect().foreach(println)
    
    //3.关闭资源
    spark.stop()
    sc.stop()

  }
}

case class User(name:String, age:Long)
