package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    //1.创建配置信息
    val conf: SparkConf = new SparkConf().setAppName("spark-streaming-test").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(4))

    //3.创建RDD队列
    val queueRDD = new mutable.Queue[RDD[Int]]()

    //4.创建queueInputstream
    val inputDStream: InputDStream[Int] = ssc.queueStream(queueRDD, oneAtATime = false)

    //5.处理队列中的数据
    val sumDStream: DStream[Int] = inputDStream.reduce(_ + _)

    //6.打印数据
    sumDStream.print()

    //7.启动任务
    ssc.start()

    //8.相队列中传入数据，模拟实际场景
    for (i <- 1 to 5) {
      queueRDD.enqueue(ssc.sparkContext.makeRDD(1 to 5))
      Thread.sleep(2000)
    }

    //9.阻塞线程并等待
    ssc.awaitTermination()
  }
}
