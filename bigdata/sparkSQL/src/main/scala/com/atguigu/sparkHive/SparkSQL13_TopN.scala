package com.atguigu.sparkHive

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkSQL13_TopN {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //注册自定义函数UDAF
    spark.udf.register("city_remark", functions.udaf(new  CityRemarkUDAF()))

    spark.sql("use test")

    //执行sql
    spark.sql(
      """
        |select area,
        |       product_name,
        |       click_cnt,
        |       city_remark
        |from (
        |         select area,
        |                product_name,
        |                click_cnt,
        |                city_remark,
        |                rank() over (partition by area order by click_cnt) rk
        |         from (
        |                  select area,
        |                         product_name,
        |                         count(*) click_cnt,
        |                         city_remark(city_name) city_remark
        |                  from (
        |                           select click_product_id,
        |                                  area,
        |                                  city_name,
        |                                  product_name
        |                           from (
        |                                    select click_product_id,
        |                                           city_id
        |                                    from user_visit_action
        |                                    where click_product_id > -1
        |                                ) v
        |                                    join city_info c on v.city_id = c.city_id
        |                                    join product_info p on p.product_id = v.click_product_id
        |                       ) t1
        |                  group by t1.area, t1.product_name
        |              ) t2
        |     )t3
        |where t3.rk <= 3
        |""".stripMargin).show(1000, false)


    //3.关闭资源
    spark.stop()
  }
}


//创建样例类
case class Buffer(var totalCnt:Long, var cityMap : mutable.Map[String, Long])

//实现自定义函数的类
case class CityRemarkUDAF() extends Aggregator[String, Buffer, String] {
  override def zero: Buffer = Buffer(0L, mutable.Map[String, Long]())

  override def reduce(buffer: Buffer, city: String): Buffer = {
    buffer.totalCnt += 1
    buffer.cityMap(city) = buffer.cityMap.getOrElse(city, 0L) + 1L
    buffer
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.totalCnt += b2.totalCnt
    for (elem <- b2.cityMap) {
      b1.cityMap(elem._1) = b1.cityMap.getOrElse(elem._1, 0L) + elem._2
    }

//    b2.cityMap.foreach({
//      case (city, cnt) => {
//        b1.cityMap(city) = b1.cityMap.getOrElse(city, 0L) + cnt
//      }
//    })
    b1
  }

  override def finish(buffer: Buffer): String = {
    val list: ListBuffer[String] = ListBuffer[String]()

    //暂存前两个总的百分比
    var sum = 0L

    //对不同城市点击数排序，并取top2
    val cityList: List[(String, Long)] = buffer.cityMap.toList.sortBy(t => t._2)(Ordering[Long].reverse).take(2)

    //对前二的城市进行处理
    cityList.foreach({
      case (city, cityCnt) => {
        val res: Long = cityCnt * 100 / buffer.totalCnt
        list.append(city + " " + res + "%")
        sum += res
      }
    })

    //计算其他城市所占的百分比
    if (buffer.cityMap.size > 2) {
      list.append("其他 " + (100 - sum) + "%")
    }
    list.mkString(",")
  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
