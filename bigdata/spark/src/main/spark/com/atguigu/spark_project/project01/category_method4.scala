package com.atguigu.spark_project.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object category_method1 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //2.1读取原始数据日志
    val rdd: RDD[String] = sc.textFile("D:\\java\\learn\\IntelljIdea\\bigdata\\spark\\spark_project\\user_visit_action.txt")

    //2.2封装样例类 将lineRDD变为actionRDD
    val actionRDD: RDD[UserVisitAction] = rdd.map(line => {
      val dataSplit: Array[String] = line.split("_")
      //将解析出来的数据封装到样例类中
      UserVisitAction(
        dataSplit(0),
        dataSplit(1),
        dataSplit(2),
        dataSplit(3),
        dataSplit(4),
        dataSplit(5),
        dataSplit(6),
        dataSplit(7),
        dataSplit(8),
        dataSplit(9),
        dataSplit(10),
        dataSplit(11),
        dataSplit(12)
      )
    })

    //转换数据结构，将actionRDD转换为CategoryCountInfo
    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(action => {
      if (action.click_category_id != "-1") {
        List(CategoryCountInfo(action.click_category_id, 1, 0, 0))
      } else if (action.order_category_ids != "null") {
        val arr: Array[String] = action.order_category_ids.split(",")
        arr.map(CategoryCountInfo(_, 0, 1, 0))
      } else if (action.pay_category_ids != "null") {
        val arr: Array[String] = action.pay_category_ids.split(",")
        arr.map(id => CategoryCountInfo(id, 0, 0, 1))
      } else {
        Nil
      }
    })

    //4 按照品类id分组,将同一个品类的数据分到同一个组内
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(_.categoryId)

    //5.将分完组后的数据进行聚合
    val reduceRDD: RDD[CategoryCountInfo] = groupRDD.mapValues(datas => {
      datas.reduce((info1, info2) => {
        info1.clickCount += info2.clickCount
        info1.orderCount += info2.orderCount
        info1.payCount += info2.payCount
        info1
      })
    }).map(_._2)

    //将聚合后的数据倒序排序
    reduceRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10).foreach(println)

    //3.关闭资源
    sc.stop()
  }
}

//用户访问动作表
case class UserVisitAction(date: String, //用户点击行为的日期
                           user_id: String, //用户的ID
                           session_id: String, //Session的ID
                           page_id: String, //某个页面的ID
                           action_time: String, //动作的时间点
                           search_keyword: String, //用户搜索的关键词
                           click_category_id: String, //某一个商品品类的ID
                           click_product_id: String, //某一个商品的ID
                           order_category_ids: String, //一次订单中所有品类的ID集合
                           order_product_ids: String, //一次订单中所有商品的ID集合
                           pay_category_ids: String, //一次支付中所有品类的ID集合
                           pay_product_ids: String, //一次支付中所有商品的ID集合
                           city_id: String) //城市 id

// 输出结果表
case class CategoryCountInfo(var categoryId: String, //品类id
                             var clickCount: Long, //点击次数
                             var orderCount: Long, //订单次数
                             var payCount: Long) //支付次数


