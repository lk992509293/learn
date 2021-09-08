package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkStreaming06_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //使用updateStateByKey必须要设置检查点目录
    ssc.checkpoint("D:\\IdeaProjects\\sparkstreamingtest\\checkpoint")

    //获取一行数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop105", 9999)

    //处理数据
    val word2oneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1))

    //用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val res: DStream[(String, Int)] = word2oneDStream.updateStateByKey(updateFunc)

    res.print()

    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc = (seq : Seq[Int], state : Option[Int]) => {
    //获取当前批次单词的和
    val sum: Int = seq.sum

    //获取历史状态的数据
    val stateCount: Int = state.getOrElse(0)

    //获取当前批次+历史批次
    Some(sum + stateCount)
  }
}
