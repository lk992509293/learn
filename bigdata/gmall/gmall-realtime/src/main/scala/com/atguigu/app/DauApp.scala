package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import org.apache.phoenix.spark._


object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.获取ssc
    val ssc = new StreamingContext(conf, Seconds(5))

    //a.从kafka获取输入流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //b.将json数据转换为样例类，并补充两个时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions((part: Iterator[ConsumerRecord[String, String]]) => {
      part.map((record: ConsumerRecord[String, String]) => {
        //将json数据转换为样例类，并补充两个时间字段
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        //补充字段
        val str: String = sdf.format(new Date(startUpLog.ts))

        //补充logdate字段
        startUpLog.logDate = str.split(" ")(0)

        //补充loghour字段
        startUpLog.logHour = str.split(" ")(1)

        startUpLog
      })
    })

    //c.先进行批次间去重，由于批次间去重一次能够去重更多的重复数据，效率更高
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)

    filterByRedisDStream.cache()
    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()

    //d.进行批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterbyGroup(filterByRedisDStream)

    //e.将去重后的数据保存至redis，供下一批数据使用
    DauHandler.saveMidToRedis(filterByGroupDStream)

    filterByGroupDStream.cache()
    filterByGroupDStream.count().print()

    //f.将数据保存至Hbase
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop105,hadoop106,hadoop107:2181"))
    })


    //3.开启执行任务,并且任务不退出
    ssc.start()
    ssc.awaitTermination()
  }
}
