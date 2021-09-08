package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    //2.创建ssc
    val ssc = new StreamingContext(conf, Seconds(5))

    //2.1获取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //2.1将JSON数据转换为样例类
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(part => {
      part.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //获取订单创建的日期和时间
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        orderInfo
      })
    })

    //2.2将数据写入Hbase
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2021_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop105,hadoop106,hadoop107:2181"))
    })


    //3.开始执行任务，并保持任务不关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
