package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(4))

    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop105", 9999)

    // 在Driver端执行，全局一次
    println("111111111:" + Thread.currentThread().getName)

    //转换为RDD执行
    val wordToSumDStream: DStream[(String, Int)] = lineDStream.transform({
      rdd => {
        // 在Driver端执行(ctrl+n JobGenerator)，一个批次一次
        println("222222:" + Thread.currentThread().getName)

        val words: RDD[String] = rdd.flatMap(_.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map({
          //在executor端每个单词到来就执行一次
          println("333333:" + Thread.currentThread().getName)
          (_, 1)
        })


        val res: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        res
      }
    })

    //打印
    wordToSumDStream.print()

    //启动streamingcontext
    ssc.start()
    ssc.awaitTermination()
  }
}
