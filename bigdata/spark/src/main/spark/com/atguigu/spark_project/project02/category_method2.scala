package com.atguigu.spark_project.project02

import com.atguigu.spark_project.project01.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object category_method2 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置文件并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("spark-core-test").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

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

    //1.创建累加器
    val acc = new CategoryCountAccumulator

    //2.注册累加器
    sc.register(acc, "acc")

    //3.使用累加器
    actionRDD.foreach(action => acc.add(action))

    //4.获取累加器的值
    val accMap: mutable.Map[(String, String), Long] = acc.value


    //5.按照商品id分组
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)

    //6.转换数据结构为（id，click，order，pay）
    val infoIt: immutable.Iterable[CategoryCountInfo] = groupMap.map({
      case (id, map) =>
        val click: Long = map.getOrElse((id, "click"), 0)
        val order: Long = map.getOrElse((id, "order"), 0)
        val pay: Long = map.getOrElse((id, "pay"), 0)
        CategoryCountInfo(id, click, order, pay)
    })

    //排序取top10
    val res: List[CategoryCountInfo] = infoIt.toList.sortBy(info => (info.clickCount, info.orderCount, info.payCount))(Ordering[(Long,Long,Long)].reverse).take(10)

    //获取top10热门品类id
    val ids: List[String] = res.map(_.categoryId)
    
    //创建广播变量
    val bdIds: Broadcast[List[String]] = sc.broadcast(ids)

    //过滤原始数据，保留热门品类top10
    val filterActionRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
      if (action.click_category_id != "-1") {
        bdIds.value.contains(action.click_category_id)
      } else {
        false
      }
    })

    //转换数据结构,并对session数进行聚合
    val reduceRDD: RDD[(String, Int)] = filterActionRDD.map(action => {
      (action.click_category_id + "=" + action.session_id, 1)
    }).reduceByKey(_ + _)

    //再次转换数据结构
    val idToSessionAndSumRDD: RDD[(String, (String, Int))] = reduceRDD.map({
      case (key, sum) => {
        val datas: Array[String] = key.split("=")
        (datas(0), (datas(1), sum))
      }
    })

    //按照品类id分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = idToSessionAndSumRDD.groupByKey()

    //对每个品类的访问人次排序，并取top10
    val result: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith(_._2 > _._2)
      }.take(10)
    )


    result.collect().foreach(println)

    //3.关闭资源
    sc.stop()
  }
}

//累加器的定义步骤：
//1.继承AccumulatorV2
//2.定义输入输出泛型
//3.实现6个抽象方法
case class CategoryCountAccumulator() extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {
  private val map: mutable.Map[(String, String), Long] = mutable.Map()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new CategoryCountAccumulator

  override def reset(): Unit = map.clear()

  override def add(action: UserVisitAction): Unit = {
    if (action.click_category_id != "-1") {
      val key: (String, String) = (action.click_category_id, "click")
      map(key) = map.getOrElse(key, 0L) + 1L
    } else if (action.order_category_ids != "null") {
      val cids: Array[String] = action.order_category_ids.split(",")
      for (cid <- cids) {
        val key: (String, String) = (cid, "order")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    } else if (action.pay_category_ids != "null") {
      val cids: Array[String] = action.pay_category_ids.split(",")
      for (cid <- cids) {
        val key: (String, String) = (cid, "pay")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach({
      case (key, cnt) =>
        map(key) = map.getOrElse(key, 0L) + cnt
    })
  }

  override def value: mutable.Map[(String, String), Long] = map
}
