package com.atguigu.sparkstreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStreaming09_reduceByKeyAndWindow_reduce {
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
    val wordToOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    // 4 窗口参数说明： 算法逻辑，窗口12秒，滑步6秒
    val resultDStream: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (a: Int, b: Int) => (a + b),
      (x: Int, y: Int) => (x - y),
      Seconds(12),
      Seconds(6),
      new HashPartitioner(2),
      (x: (String, Int)) => x._2 > 0
    )
    
    //打印结果
    resultDStream.print()

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
