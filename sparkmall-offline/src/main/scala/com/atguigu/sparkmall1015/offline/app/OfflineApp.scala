package com.atguigu.sparkmall1015.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall1015.common.bean.UserVisitAction
import com.atguigu.sparkmall1015.common.util.{JdbcUtil, PropertiesUtil}
import com.atguigu.sparkmall1015.offline.acc.CategoryCountAccumulator
import com.atguigu.sparkmall1015.offline.bean.CategoryCount
import com.atguigu.sparkmall1015.offline.handler.{CategoryCountHandler, PageConvertRatioHandler, Top10CategoryTop10SessionHandler}
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
    val userVisitActionRdd: RDD[UserVisitAction] = sparkSession.sql(sql).as[UserVisitAction].rdd

    //需求一  十大热门品类
   // val categoryCountTop10List: List[CategoryCount] = CategoryCountHandler.handle(sparkSession,userVisitActionRdd,taskId)
    println("需求一 完成")
    //需求二  十大热门品类的十大活跃session
   // Top10CategoryTop10SessionHandler.handle(sparkSession,userVisitActionRdd,categoryCountTop10List,taskId)
    println("需求二 完成")

    //需求四 页面单跳转化率
    PageConvertRatioHandler.handle(sparkSession,userVisitActionRdd,taskId)
    println("需求四 完成")

  }

}
