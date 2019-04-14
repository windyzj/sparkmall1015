package com.atguigu.sparkmall1015.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall1015.common.util.RedisUtil
import com.atguigu.sparkmall1015.realtime.bean.AdsLog
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsCountHandler {
//  dstream[adsLog] 原始数据
//
//  .window()
//  得到近一小时的所有访问数据
//  计算每广告每小时每分钟的点击量
//  近一小时的dstream[adsLog]
//  =>map
//  =>
//    dstream(ads_hour_minute,1L)
//  => reducebykey
//  =>RDD[(adshourminus,count))]
//  =>map{}
//  =>
//  RDD[(ads,(hourminus,count))]
//  groupbykey
//  =>
//    RDD[ads:String, Iterable[(hourminus,count)]]
//  =>
//  =>json4s
//  =>
//    RDD[ads:String, hourMinutesCountJson: String]
//  =>
//  Map[ads:String, hourMinutesCountJson: String]
//  =>
//  hmset(last_hour_ads_click :String,Map[ads:String, hourMinutesCountJson: String])

  def handle(filteredAdslogDstream: DStream[AdsLog]): Unit ={
    // 利用 滑动窗口取  近一小时的dstream[adsLog]
    val lastHourAdsLogDstream: DStream[AdsLog] = filteredAdslogDstream.window(Minutes(60),Seconds(10))
     // 按照每广告每小时每分钟进行统计  1.调整key 2 reducebykey
    val adsHourMinuteCountDStream: DStream[(String, Long)] = lastHourAdsLogDstream.map { adsLog =>
      val hourMinute: String = new SimpleDateFormat("HH:mm").format(new Date(adsLog.ts))
      val key = adsLog.adsId + "_" + hourMinute
      (key, 1L)
    }.reduceByKey(_ + _)
    // 调整结构进行  按广告ads分组
    val hourMinuteCountByAdsDstream: DStream[(String, Iterable[(String, Long)])] = adsHourMinuteCountDStream.map { case (adsHourMinute, count) =>
      val adsHourMinuteArr: Array[String] = adsHourMinute.split("_")
      val ads: String = adsHourMinuteArr(0)
      val hourMinute: String = adsHourMinuteArr(1) // RDD[(ads,(hourminus,count))]
      (ads, (hourMinute, count))
    }.groupByKey()
    // 把小时分钟的计数 转成json
    val hourMinuteCountJsonByAdsDstream: DStream[(String, String)] = hourMinuteCountByAdsDstream.map { case (ads, hourMinuteCountItr) =>
      import org.json4s.JsonDSL._
      val hourMinuteCountJson: String = JsonMethods.compact(JsonMethods.render(hourMinuteCountItr))
      (ads, hourMinuteCountJson)
    }
     hourMinuteCountJsonByAdsDstream.foreachRDD{rdd=>
          val array: Array[(String, String)] = rdd.collect()
          val jedis: Jedis = RedisUtil.getJedisClient
         val key = "last_hour_ads_click"
         import  collection.JavaConversions._
          jedis.hmset(key,array.toMap)
           jedis.close()
   }
    //保存到redis中
 //   hourMinuteCountJsonByAdsDstream.foreachRDD{rdd=>
//


  //    rdd.foreachPartition{ hourminuteCountJsonByAdsItr=>
//
  //      val filteredItr: Iterator[(String, String)] = hourminuteCountJsonByAdsItr.filter{tt=> (tt!=null&& !tt._1.trim.isEmpty)}
//        val key = "last_hour_ads_click"
  //      println("1111="+filteredItr.mkString("\n"))
//        import  collection.JavaConversions._
//      // println( hourminuteCountJsonByAdsItr.toMap.mkString("\n"))
//        if(hourminuteCountJsonByAdsItr!=null&&hourminuteCountJsonByAdsItr.size>0){
//         println( hourminuteCountJsonByAdsItr.toMap.mkString("\n"))
////          val jedis: Jedis = RedisUtil.getJedisClient
////          jedis.hmset(key,hourminuteCountJsonByAdsItr.toMap)
////          jedis.close()
         }
//
 //      }
//    }
 // }
}
