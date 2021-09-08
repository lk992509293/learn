package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_FliePartitions {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FileParallelism")
    val sc = new SparkContext(sparkConf)

    /*
    文件1：
    12@@  => 0,1,2,3
    234   => 4,5,6
    文件2：
    1@@   => 0,1,2
    2@@   => 3,4,5
    3@@   => 6,7,8
    4     => 9
    计算过程：
    1.字节总数：7+10=17
    2.每个分区字节数：17/3=5 .. 2
    3.文件1：
       分区0：(0,5) =>12 234
       分区1：(5,7) =>空
     文件2：
       分区0：(0,5)  =>1 2
       分区1：(5,10) => 3 4
     */
    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\input1",3)
    rdd.saveAsTextFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\output")
    sc.stop()

  }
}
