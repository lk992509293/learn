package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  //进行批次内去重
  def filterbyGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //1.将数据转换为k-v结构
    val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions((part: Iterator[StartUpLog]) => {
      part.map(log => {
        ((log.mid, log.logDate), log)
      })
    })

    //2.将key相同的k-v聚合在一起
    val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

    //3.将数据排序并取第一条数据
    val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //4.将数据扁平化
    val value: DStream[StartUpLog] = midAndDateToLogListDStream.flatMap(_._2)

    value
  }

  //进行批次间去重
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    //方案三:在每个批次内创建一次连接，来优化连接个数
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {

      //1.获取redis连接
      val jedis = new Jedis("hadoop105", 6379)

      //2.查看Redis中的mid
      //获取rediskey
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))

      val mid: util.Set[String] = jedis.smembers(redisKey)

      //3.将数据广播至executor端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mid)

      //4.根据获取到的mid去重
      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })

      //关闭连接
      jedis.close()
      midFilterRDD
    })

    value
  }

  /**
   * 将去重后的数据保存至Redis，为了下一批数据去重用
   *
   * @param startUpLogDStream
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        //1.创建连接
        val jedis = new Jedis("hadoop105", 6379)

        //2.写入数据
        part.foreach(log => {
          //redisKey
          val redisKey: String = "DAU:" + log.logDate

          //将mid存入库中
          jedis.sadd(redisKey, log.mid)
        })

        //关闭连接
        jedis.close()
        jedis.close()
      })
    })
  }
}
