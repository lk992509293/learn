package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    // 保存数据到检查点
    ssc.checkpoint("./ck")

    //读取监控到的数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop105", 9999)

    //切割变换
    val wordToOne : DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    //窗口参数：窗口步长12秒，滑动步长6秒
    val wordCounts: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(12), Seconds(6))

    //打印输出
    wordCounts.print()

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
