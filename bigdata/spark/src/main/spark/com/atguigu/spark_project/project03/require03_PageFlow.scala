package com.atguigu.spark_project.project03

import com.atguigu.spark_project.project01.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require03_PageFlow {
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

    //准备过滤数据，对分母过滤
    val ids = List("1", "2", "3", "4", "5", "6", "7")

    //创建分子过滤集合
    val zipIds: List[String] = ids.zip(ids.tail).map {
      case (p1, p2) => p1 + "-" + p2
    }

    //创建广播变量
    val bdIds: Broadcast[List[String]] = sc.broadcast(ids)

    //计算分母
    //过滤出要统计page_id,并计算这个page_id被访问了多少次
    val idsArr: Array[(String, Int)] = actionRDD
      .filter(action => bdIds.value.init.contains(action.page_id))
      .map(action => (action.page_id, 1))
      .reduceByKey(_ + _).collect()

    //把数据结构转换为map，方便使用
    val denominator: Map[String, Int] = idsArr.toMap


    //计算分子，分子的数据结构为（1-2，count)
    //对过滤后的数据按照session_id分组
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    //将分组后的数据按照时间升序排序
    val page2pageRDD: RDD[(String, List[String])] = sessionGroupRDD.mapValues(datas => {
      //1 将迭代器转成list,然后按照行动时间升序排序
      val actions: List[UserVisitAction] = datas.toList.sortBy(_.action_time)
      //2 转变数据结构,获取到pageId
      val pageidList: List[String] = actions.map(_.page_id)
      //3 将排好序的页面zip拉链获得单跳元组
      val pageToPageList: List[(String, String)] = pageidList.zip(pageidList.tail)
      //4 转换数据结构
      val pageJumpCounts: List[String] = pageToPageList.map({ case (p1, p2) => p1 + "-" + p2 })
      //5 对分子过滤，只保留下1-2,2-3,3-4,4-5,5-6,6-7的数据
      pageJumpCounts.filter(str => zipIds.contains(str))
    })
    
    //转换数据结构
    val pageFlowRDD: RDD[List[String]] = page2pageRDD.map(_._2)

    //聚合统计结果
    val reduceFlowRDD: RDD[(String, Int)] = pageFlowRDD.flatMap(list => list).map((_, 1)).reduceByKey(_ + _)

    //通过计算出来的分子和分母，计算单跳转化率
    reduceFlowRDD.foreach({
      case (pageFlow, sum) => {
        val pageIds: Array[String] = pageFlow.split("-")

        //根据pageFlow取出分母的值
        val pageIdSum: Int = denominator.getOrElse(pageIds(0), 1)

        //计算转化率
        println(pageIdSum  + " = " + sum.toDouble/pageIdSum * 100 + "%")
      }
    })

    //3.关闭资源
    sc.stop()
  }
}
