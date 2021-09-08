package com.atguigu.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object MyPartitionerTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("myPartitioner").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (0, "fff"), (4, "ggg")), 3)

    val unit: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    unit.mapPartitionsWithIndex((index, it) => it.map((index, _))).collect.foreach(println)

    sc.stop()
  }
}

class MyPartitioner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case key: Int if key <= 1 => 0
      case key: Int if key > 1 => 1
      case _ => 0
    }
  }
}
