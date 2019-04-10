package com.atguigu.sparkmall1015.offline.handler

import com.atguigu.sparkmall1015.common.bean.UserVisitAction
import com.atguigu.sparkmall1015.common.util.JdbcUtil
import com.atguigu.sparkmall1015.offline.bean.{CategoryCount, Top10Session}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Top10CategoryTop10SessionHandler {

  //    1、 过滤 ：  只保留所有点击了十大品类的用户action    filter=>   RDD[UserVisitAction]

  //    2、 按照品类和sessionId进行分组 =>(cid+sessionId,1L) .reducebykey  => RDD[cid_sessionid,count]
  //
  //    3、  map=> RDD[cid ,(sessionId,count)]=>groupbykey => RDD[cid,iterable[(sessionId,count)]]
  //
  //      4 RDD[cid,iterable[(sessionId,count)]] .map{   1 iterable 进行排序 ，2  iterable 截取前十 }
  //
  //    5 RDD[cid,iterable[(sessionId,count)]] => 调整结构   Array(taskId,categoryId,sessionId,count) 保存到数据库中
  def handle(sparkSession: SparkSession,userVisitActionRdd:RDD[UserVisitAction],categoryCountTop10List: List[CategoryCount],taskId:String): Unit ={
//    1、 过滤 ：  只保留所有点击了十大品类的用户action    filter=>   RDD[UserVisitAction]
    val top10CidList: List[String] = categoryCountTop10List.map{_.cid}
    val categoryCountTop10ListBC: Broadcast[List[String]] = sparkSession.sparkContext.broadcast(top10CidList)
    val filteredUserActionRDD: RDD[UserVisitAction] = userVisitActionRdd.filter { userVisitAction =>
      categoryCountTop10ListBC.value.contains(userVisitAction.click_category_id.toString)
    }


    //    2、 按照品类和sessionId进行分组 =>(cid+sessionId,1L) .reducebykey  => RDD[cid_sessionid,count]
    //得到每个品类每个session的点击次数
    val cidSessionCountRDD: RDD[(String, Long)] = filteredUserActionRDD.map(userAction=>(userAction.click_category_id+"_"+userAction.session_id,1L)).reduceByKey(_+_)

    //    3、 调整分组 ， 以cid为组 ，为取每个组内前十做准备
    // map=> RDD[cid ,Top10Session]=>groupbykey => RDD[cid,iterable[Top10Session]]
    val sessionCountByCidRDD: RDD[(String, Iterable[Top10Session])] = cidSessionCountRDD.map { case (cidSessionId, count) =>
      val cid: String = cidSessionId.split("_")(0)
      val sessionId: String = cidSessionId.split("_")(1)
      (cid, Top10Session(taskId,cid,sessionId, count)  )
    }.groupByKey()

    //   4  针对每个cid组 的session集合 取前十名
    // RDD[cid,iterable[(sessionId,count)]] .map{   1 iterable 进行排序 ，2  iterable 截取前十 }
    val top10SessionCountByCidRDD: RDD[(String, List[Top10Session])] = sessionCountByCidRDD.map { case (cid, sessionItr) =>
      //排序  截取前十
      val top10SessionCountList: List[Top10Session] = sessionItr.toList.sortWith(_.clickcount > _.clickcount).take(10)
      (cid, top10SessionCountList)

    }



    //    5 RDD[cid,iterable[(sessionId,count)]] => 调整结构   Array(taskId,categoryId,sessionId,count) 保存到数据库中
    val top10SessionRDD: RDD[Array[Any]] = top10SessionCountByCidRDD.flatMap { case (cid, top10SessionList) =>
      val top10SessionArrayList: List[Array[Any]] = top10SessionList.map { top10sessionCount => Array(top10sessionCount.taskId, top10sessionCount.categoryId, top10sessionCount.sessionId, top10sessionCount.clickcount)
      }
      top10SessionArrayList
    }
    // 从rdd中收集到driver中
    val top10SessionListForSave: List[Array[Any]] = top10SessionRDD.collect().toList


    //6 保存到数据库中
    JdbcUtil.executeBatchUpdate("insert into top10_session values(?,?,?,?)",top10SessionListForSave)

  }

}
