package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-streaming-test").setMaster("local[*]")

    //2.创建sparkstreaming
    val ssc = new StreamingContext(conf,Seconds(3))

    //通过端口读取一行数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop105", 9999)

    //对数据进行扁平化处理
    val wordDStream: DStream[String] = lineDStream.flatMap(line => {
      println("111111111111111111111")
      line.split(" ")
    })
    
    //转换数据格式
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    
    //统计每个单词出现的次数
    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    //打印数据
    wordToSumDStream.print()

    //3.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
