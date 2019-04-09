package com.atguigu.sparkmall1015.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall1015.common.bean.UserVisitAction
import com.atguigu.sparkmall1015.common.util.{JdbcUtil, PropertiesUtil}
import com.atguigu.sparkmall1015.offline.acc.CategoryCountAccumulator
import com.atguigu.sparkmall1015.offline.bean.CategoryCount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

object OfflineApp {

  def main(args: Array[String]): Unit = {
    val taskId: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("offline_app")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val properties: Properties = PropertiesUtil.load("config.properties")

    val conditionsProp: Properties = PropertiesUtil.load("conditions.properties")
    val conditionsJson: String = conditionsProp.getProperty("condition.params.json")
    val conditionJSONobj: JSONObject = JSON.parseObject(conditionsJson)
    //startDate:"2018-11-01", \
//    endDate:"2018-12-28", \
//    startAge: "20", \
//    endAge: "50", \

    //1  进行过滤
    val startDate: String = conditionJSONobj.getString("startDate")
    val endDate: String = conditionJSONobj.getString("endDate")
    val startAge: String = conditionJSONobj.getString("startAge")
    val endAge: String = conditionJSONobj.getString("endAge")
    val database=  properties.getProperty("hive.database");
    sparkSession.sql("use "+database);
    var sql=" select uv.*  from user_visit_action  uv   join  user_info  ui on uv.user_id=ui.user_id  where 1=1 "
    if(startDate!=null && startDate.length>0){
      sql+=" and uv.date >= '"+startDate+"'"
    }
    if(endDate!=null && endDate.length>0){
      sql+=" and uv.date <='"+endDate+"'"
    }
    if(startAge!=null && startAge.length>0){
      sql+=" and ui.age >="+startAge
    }
    if (endAge!=null && endAge.length>0){
      sql+=" and ui.age<="+endAge
    }
    import  sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql).as[UserVisitAction].rdd

    val accumulator = new CategoryCountAccumulator
    sparkSession.sparkContext.register(accumulator)
  //2 利用累加器进行统计操作 ，得到一个map结构的统计结果
    rdd.foreach { userVisitAction =>
      if (userVisitAction.click_category_id != -1L) {
        accumulator.add(userVisitAction.click_category_id + "_click")

      } else if (userVisitAction.order_category_ids != null) {
        val cidArray: Array[String] = userVisitAction.order_category_ids.split(",")
        for (cid <- cidArray) {
          accumulator.add(cid + "_order")
        }

      } else if (userVisitAction.pay_category_ids != null) {
        val cidArray: Array[String] = userVisitAction.pay_category_ids.split(",")
        for (cid <- cidArray) {
          accumulator.add(cid + "_pay")
        }


      }
    }
    val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value
    println(categoryCountMap.mkString("\n"))
                                //cid=1
    val countGroupbyCidMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy{case (cid_action,count)=>cid_action.split("_")(0) }
    //调整结构
    val categoryCountItr: immutable.Iterable[CategoryCount] = countGroupbyCidMap.map { case (cid, countMap) =>
      CategoryCount(taskId, cid, countMap.getOrElse(cid + "_click", 0L), countMap.getOrElse(cid + "_order", 0L), countMap.getOrElse(cid + "_pay", 0L))

    }
    //取前十
    val sortedCategoryCountList: List[CategoryCount] = categoryCountItr.toList.sortWith { (categoryCount1, categoryCount2) =>
      if (categoryCount1.clickCount > categoryCount2.clickCount) {
        true
      } else if (categoryCount1.clickCount == categoryCount2.clickCount) {
        if (categoryCount1.orderCount > categoryCount2.orderCount) {
          true
        } else {
          false
        }
      }
      else {
        false
      }
    }
    //截取前十
    val top10List: List[CategoryCount] = sortedCategoryCountList.take(10)
     // 调整成array
    val top10ArrayList: List[Array[Any]] = top10List.map{categoryCount=>Array(categoryCount.taskId,categoryCount.cid,categoryCount.clickCount,categoryCount.orderCount,categoryCount.payCount)}
     //保存到mysql中
     JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",top10ArrayList)

  }

}
