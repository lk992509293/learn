package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置属性
    val conf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建ssc
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.获取kafka中的数据
    val orderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    val orderDetaiDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将数据转换为样例类
    //a.转换订单表数据
    val idToInfoDStream: DStream[(String, OrderInfo)] = orderDStream.mapPartitions((part: Iterator[ConsumerRecord[String, String]]) => {
      part.map((record: ConsumerRecord[String, String]) => {
        //将json数据转换为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补充数据字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        //返回数据
        (orderInfo.id, orderInfo)
      })
    })

    //b.转换订单明细数据
    val idToDetailDStream: DStream[(String, OrderDetail)] = orderDetaiDStream.mapPartitions((part: Iterator[ConsumerRecord[String, String]]) => {
      part.map((record: ConsumerRecord[String, String]) => {
        //将json数据转换为样例类
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (orderDetail.order_id, orderDetail)
      })
    })

    //5.使用fullouterjoin进行双流join，防止数据丢失
    val fullDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToInfoDStream.fullOuterJoin(idToDetailDStream)

    //6.操作数据
    val noUserDStream: DStream[SaleDetail] = fullDStream.mapPartitions((iter: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      //创建集合，用来存放关联上去的数据
      val saleDetails: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建redis连接
      val jedis = new Jedis("hadoop105", 6379)

      //遍历迭代器数据
      iter.foreach({
        case (orderId, (orderOpt, detailOpt)) => {
          //创建redisKey
          val orderInfoKey: String = "orderInfo:" + orderId
          val orderDetailKey: String = "orderDetail:" + orderId

          //a.判断是否有orderInfo数据
          if (orderOpt.isDefined) {
            //有orderInfo数据
            val orderInfo: OrderInfo = orderOpt.get

            //b.判断orderDetail是否为空
            if (detailOpt.isDefined) {
              //有orderDetail数据
              val orderDetail: OrderDetail = detailOpt.get
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              //将订单数据和订单明细表数据保存到缓存中
              saleDetails.add(saleDetail)
            }

            //c.将数据转换为json格式保存到redis中
            val orderInfoJson: String = Serialization.write(orderInfo)
            //orderInfo数据保存至redis
            jedis.set(orderInfoKey, orderInfoJson)
            //设置过期时间
            jedis.expire(orderInfoKey, 120)

            //d.去缓冲查询订单明细数据
            if (jedis.exists(orderDetailKey)) {
              //缓存中存在该订单明细
              val detailSet: util.Set[String] = jedis.smembers(orderDetailKey)
              //将redis中的json数据转换为样例类
              detailSet.asScala.foreach((detail: String) => {
                val orderDetail: OrderDetail = JSON.parseObject(detail, classOf[OrderDetail])
                saleDetails.add(new SaleDetail(orderInfo, orderDetail))
              })
            }
          }
          //如果没有orderInfo数据,就需要去缓存中拿orderinfo数据join
          else {
            //a.拿到detail数据
            val orderDetail: OrderDetail = detailOpt.get
            if (jedis.exists(orderInfoKey)) {
              //缓存中存在orderinfo
              //a.1获取orderinfo数据
              val orderJSON: String = jedis.get(orderInfoKey)

              //a.2将json数据转换为样例类
              val orderInfo: OrderInfo = JSON.parseObject(orderJSON, classOf[OrderInfo])

              //a.3将样例类添加到SaleDetail样例类集合里
              saleDetails.add(new SaleDetail(orderInfo, orderDetail))
            }
            //如果缓存中不存在orderinfo，直接把detail数据写入缓存中
            else {
              //a.将数据转换为JSON数据
              val orderDetailJson: String = Serialization.write(orderDetail)

              //b.将数据写入redis中
              jedis.set(orderDetailKey, orderDetailJson)

              //c.设置过期时间
              jedis.expire(orderDetailKey, 120)
            }
          }
        }
      })

      //关闭redis连接
      jedis.close()
      saleDetails.asScala.toIterator
    })

    noUserDStream.print(100)

    //7.补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions((part: Iterator[SaleDetail]) => {
      //a.获取redis连接
      val jedis = new Jedis("hadoop105", 6379)

      //b.查库
      val details: Iterator[SaleDetail] = part.map((saleDetail: SaleDetail) => {
        //根据key获取数据
        val userInfoKey: String = "userInfo:" + saleDetail.user_id
        val userInfoJson: String = jedis.get(userInfoKey)
        //将数据转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })

      //关闭连接
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //8.将数据写入es
    //修改数据格式
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD((rdd: RDD[SaleDetail]) => {
      rdd.foreachPartition((part: Iterator[SaleDetail]) => {
        //创建索引名
        val indexName: String = GmallConstants.ES_DETAIL_INDEXNAME + "-" + sdf.format(new Date(System.currentTimeMillis()))

        //转换数据格式
        val list: List[(String, SaleDetail)] = part.toList.map((saleDetail: SaleDetail) => {
          (saleDetail.order_detail_id, saleDetail)
        })

        //将数据写入es
        MyEsUtil.insertBulk(indexName, list)
      })
    })

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
