package com.atguigu.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming11_stop {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //读取数据
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop105", 9999)

    //切分并转换数据
    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    //开启监控
    new Thread(new MonitorStop(ssc)).start()

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}

class MonitorStop(ssc: StreamingContext) extends Runnable {
  override def run(): Unit = {
    //获取HDFS文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop105:8020"), new Configuration(), "atguigu")

    while (true) {
      Thread.sleep(5000)

      val res: Boolean = fs.exists(new Path("hdfs://hadoop105:8020/stopSpark"))

      if (res) {
        val state: StreamingContextState = ssc.getState()
        if (state == StreamingContextState.ACTIVE) {
          //优雅关闭
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}
