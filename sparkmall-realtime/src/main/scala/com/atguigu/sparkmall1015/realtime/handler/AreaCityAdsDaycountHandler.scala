package com.atguigu.sparkmall1015.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall1015.common.util.RedisUtil
import com.atguigu.sparkmall1015.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsDaycountHandler {


//  1 、 要把原有的数据结构转换为 kv结构
//    map=>    key:   area:city:ads:date  value: 1L
//
//  2 updateStatebykey(Seq[Long] ,Option[Long]=>  )
//
//  seq  所有相同的key的value会放到seq
//    option   当前已有的key的累计值
//    实现方法： 将seq 汇总  合并到累计值中  返回一个新的累计值
//
//  得到一个有累计值的Dstream
//  3  把流中的结果刷新到redis中
  def handle(filteredAdslogDstream: DStream[AdsLog],sparkContext: SparkContext): DStream[(String, Long)] ={
  //  1 、 要把原有的数据结构转换为 kv结构
  //    map=>    key:   area:city:ads:date  value: 1L
    val areaCityAdsDateDstream: DStream[(String, Long)] = filteredAdslogDstream.map { adsLog: AdsLog =>
      val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(adsLog.ts))
      val key = adsLog.area + ":" + adsLog.city + ":" + adsLog.adsId + ":" + date
      (key, 1L)
    }
  //  2 updateStatebykey(Seq[Long] ,Option[Long]=>  )
  //
  //  seq  所有相同的key的value会放到seq
  //    option   当前已有的key的累计值
  //    实现方法： 将seq 汇总  合并到累计值中  返回一个新的累计值
  //
  sparkContext.setCheckpointDir("./checkpoint")
  val areaCityAdsDaycountDstream: DStream[(String, Long)] = areaCityAdsDateDstream.updateStateByKey { (valueSeq: Seq[Long], totalOption: Option[Long]) =>
    println(valueSeq.mkString(","))
    val sum: Long = valueSeq.sum
    val total: Long = totalOption.getOrElse(0L) + sum
    Some(total)
  }
  //  3  把流中的结果刷新到redis中

   // driver 中执行   只有启动时执行一次
  areaCityAdsDaycountDstream.foreachRDD{rdd=>
    //driver 中执行   每个周期执行一次

    rdd.foreachPartition{areaCityAdsCountItr =>   //executor   每个分区执行一次

      val redisKey="area_city_ads_daycount"
      val areaCityAdsCountMap: Map[String, String] = areaCityAdsCountItr.map{
        case (key,count)=>(key,count.toString)  //executor  每个元素执行一次
       }.toMap
          import  scala.collection.JavaConversions._   //由于jedis支持的map是java的map ,所有要引用隐式转换把scala的map转换成java版本的
          // 批量保存
          if(areaCityAdsCountMap!=null&&areaCityAdsCountMap.size>0){
            val jedis: Jedis = RedisUtil.getJedisClient
            jedis.hmset(redisKey,areaCityAdsCountMap)
            jedis.close
          }
      }

    }
  areaCityAdsDaycountDstream



  }

}
