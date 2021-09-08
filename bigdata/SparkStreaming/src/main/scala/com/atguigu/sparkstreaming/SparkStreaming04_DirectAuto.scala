package com.atguigu.sparkstreaming

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {
    //创建SparkStreaming配置文件
    val conf: SparkConf = new SparkConf().setAppName("spark-streaming-test").setMaster("local[*]")

    //创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(4))

    //定义kafka参数，“->”方法是所有Scala对象都有的方法，返回一个二元的元组(A,B)，例如：scala> 1 -> 2，res9: (Int, Int) = (1,2)
    val kafkaParams : Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop105:9092,hadoop106:9092,hadoop107:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "testTopic",
      //"auto.offset.reset" -> "latest",
      //"enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //读取kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaParams) //消费策略
    )

    //将每条消息(KV)的V取出
    val valueDStream: DStream[String] = kafkaDStream.map(_.value())

    //计算wordcount
    valueDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
