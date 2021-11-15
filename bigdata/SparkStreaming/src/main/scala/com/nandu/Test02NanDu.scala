package com.nandu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test02NanDu {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点目录
    ssc.checkpoint("checkpoint")

    //获取一行数据
    val lineRDD: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //定义状态更新方法


    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
