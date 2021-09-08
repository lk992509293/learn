package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object SparkSQL11_MySQL_Write {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark-sql-test").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val rdd: RDD[User] = spark.sparkContext.makeRDD(List(User("zhangsan", 3000), User("lisi", 3001)))

    //声明隐式转换
    import spark.implicits._

    val ds: Dataset[User] = rdd.toDS()

    //向mysql中写入数据
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop105:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "test")
      .mode(SaveMode.Append)
      .save()

    //释放资源
    spark.stop()
  }
}
