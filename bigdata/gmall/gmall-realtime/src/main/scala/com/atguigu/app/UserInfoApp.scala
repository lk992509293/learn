package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {


    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建StreaminhgContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.获取kafka数据
    val userDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //4.获取userInfo数据
    val infoJson: DStream[String] = userDStream.map(record => record.value())

    //5.将数据写入Redis
    infoJson.foreachRDD(rdd => {
      rdd.foreachPartition((partition: Iterator[String]) => {
        //a.获取连接
        val jedisClient: Jedis = new Jedis("hadoop105", 6379)
        //b.写库
        partition.foreach(info => {
          val userInfo: UserInfo = JSON.parseObject(info, classOf[UserInfo])
          val userInfoKey: String = "userInfo:" + userInfo.id
          val str: String = jedisClient.set(userInfoKey, info)
        })

        //c.关闭连接
        jedisClient.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

