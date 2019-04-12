package com.atguigu.sparkmall1015.offline.handler

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall1015.common.bean.UserVisitAction
import com.atguigu.sparkmall1015.common.util.{JdbcUtil, PropertiesUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConvertRatioHandler {

  /***
    * 需求四 ： 页面单跳转化率   =   规定的页面跳转次数 （） / 规定页面的访问次数
要得到  规定的页面跳转次数 的 k v结构的map   各个跳转的次数  和 规定页面的访问次数   的map   各个单页的访问次数

    */
  def handle(sparkSession: SparkSession,userVisitActionRDD:RDD[UserVisitAction],taskId:String): Unit ={
//    1、 各个单页的访问次数
//
//    user_visit_action
//    1.1  过滤    截取 要统计的单页 列表
//    用来过滤    保留所有规定页面的访问  ，  进行统计，得到每个单页访问的次数
//    用broadcast 传递要统计的单页列表
//      rdd.filter ( bc.value.contains(xx)  )
//    1.2  调整结构   以pageid为key的元组结构  (pageId,1L) .countByKey        map

      //得到过滤的依据
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val jsonConfig: String = properties.getProperty("condition.params.json")
    val jSONObject: JSONObject = JSON.parseObject(jsonConfig)
    val targetPageFlow: String = jSONObject.getString("targetPageFlow")
    val pageArray: Array[String] = targetPageFlow.split(",")  // 1,2,3,4,5,6,7
    val targetPageArray: Array[String] = pageArray.slice(0,pageArray.length-1)  //1,...6
    val targetPageArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageArray)
    //    用来过滤    保留所有规定页面的访问  ，  进行统计，得到每个单页访问的次数
    //    用broadcast 传递要统计的单页列表
    val targetPageActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>
      targetPageArrayBC.value.contains(userVisitAction.page_id.toString)
    }
    //1.2  调整结构   以pageid为key的元组结构  (pageId,1L) .countByKey        map
    //得到各个规定单页的访问次数清单
    //分母
    val pageVisitCountMap: collection.Map[Long, Long] = targetPageActionRDD.map{userAction=>(userAction.page_id,1L)}.countByKey()


    // 2 v
//    1 、过滤
//    过滤依据   1-2,2-3,......
//    // 1,2,3,4,5,6,7   => //1-2,2-3,...6-7
//    (1,2,3,4,...6).zip(2,3,...7) =>   (1,2),(2,3),(3,4)....=> map //1-2,2-3,...6-7
    val nextPageArray: Array[String] = pageArray.slice(1,pageArray.length)
    val pageJumpArray: Array[String] = targetPageArray.zip(nextPageArray).map{case (pageId,nextPageId)=> pageId+"-"+nextPageId}  //1-2,2-3,...6-7
    val pageJumpArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageJumpArray)

    //2  统计各个跳转的次数 1-2  2-3  4-5 ，5-8
//    2.1  按照session进行分组
//      2.2  排序
//      2.3  调整结构 得到用户 session访问清单  1,2,4,5,8,12,33
//    2.4  调整结构  1-2,2-4,4-5,5-8,8-12...
//    2.5  根据规定跳转的页面 进行过滤    不需要统计的跳转过滤掉  1-2 ，4-5
//    2.6  把所有跳转进行统计   1-2 100,2-3 80， 3-4 50 ......  map


    //2.1  按照session进行分组
    val userActionGroupbySessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.map{userAction=>(userAction.session_id,userAction)}.groupByKey()
    // 2.2  排序
    val pageJumpRDD: RDD[String] = userActionGroupbySessionRDD.flatMap { case (sessionId, actionItr) =>
      val sortedActionsList: List[UserVisitAction] = actionItr.toList.sortWith(_.action_time < _.action_time)
      //      2.3  调整结构 得到用户 session访问清单  1,2,4,5,8,12,33
      val pageIdList: List[Long] = sortedActionsList.map(_.page_id)
      val fromPageList: List[Long] = pageIdList.slice(0, pageIdList.length - 1)
      val toPageList: List[Long] = pageIdList.slice(1, pageIdList.length)
      //    2.4  调整结构  1-2,2-4,4-5,5-8,8-12...
      val pageJumpList: List[String] = fromPageList.zip(toPageList).map { case (frompage, topage) => frompage + "-" + topage }
     // 2.5  根据规定跳转的页面 进行过滤    不需要统计的跳转过滤掉  1-2 ，4-5
      val filteredPageJumpList: List[String] = pageJumpList.filter { pageJump =>
        pageJumpArrayBC.value.contains(pageJump)
      }
      filteredPageJumpList //一个session中跳转
    }
    //    2.6  把所有跳转进行统计   1-2 100,2-3 80， 3-4 50 ......  map
    val pageJumpCountMap: collection.Map[String, Long] = pageJumpRDD.map((_,1L)).countByKey()  //分子

    //3   分子/分母  计算比例
    val pageJumpRatioItr: Iterable[Array[Any]] = pageJumpCountMap.map { case (pageJump, pageJumpCount) =>
      val fromPageId: String = pageJump.split("-")(0)
      val pageVisitCount: Long = pageVisitCountMap.getOrElse(fromPageId.toLong, 10000L) //分母
    val pageJumpRatio: Double = Math.round(pageJumpCount.toDouble * 1000 / pageVisitCount) / 10D
      Array(taskId, pageJump, pageJumpRatio)
    }

    // 4 保存到mysql中
    JdbcUtil.executeBatchUpdate("insert into page_convert_ratio values(?,?,?)",pageJumpRatioItr)




  }


  def main(args: Array[String]): Unit = {
    println(Array(1, 2, 3, 4).slice(0, 3).mkString(","))
  }
}
