package com.atguigu.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(4))

    //创建自定义的receiver的Streaming
    val lineDStream : ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop105", 9999))

    //将每一行数据做切分
    val wordDStream: DStream[String] = lineDStream.flatMap(line => line.split(" "))

    //转换数据结构
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    
    //统计每个单词出现的次数
    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    //打印
    wordToSumDStream.print()

    //启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}

class CustomerReceiver(host : String, port : Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //receiver刚启动的时候调用此方法，读取数据并将数据发送给spark
  override def onStart(): Unit = {
    new Thread("receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()


  }

  override def onStop(): Unit = {}

  //读取数据，并将数据发送给spark
  def receive (): Unit = {
    //创建一个socket
    val sc = new Socket(host, port)

    //字节流读取数据不方便,转换成字符流buffer,方便整行读取
    val reader = new BufferedReader(new InputStreamReader(sc.getInputStream))

    //读取数据
    var input: String = reader.readLine()

    //当receiver没有关闭并且输入的数据部为null时，就继续读取数据
    while (!isStopped() && input != null) {
      store(input)
      input = reader.readLine()
    }

    //如果循环结束，则关闭资源
    reader.close()
    sc.close()
  }
}
