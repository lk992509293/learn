package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //读取数据
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop105", 9999)

    //处理数据
    val wordToOneDStream : DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1))

    //获取窗口返回数据
    val wordToOneByWindow: DStream[(String, Int)] = wordToOneDStream.window(Seconds(12), Seconds(6))

    //聚合窗口数据并打印
    wordToOneByWindow.reduceByKey(_ + _).print()

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
