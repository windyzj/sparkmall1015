package com.atguigu.sparkmall1015.realtime.app

import com.atguigu.sparkmall1015.common.util.MyKafkaUtil
import com.atguigu.sparkmall1015.realtime.bean.AdsLog
import com.atguigu.sparkmall1015.realtime.handler.{AreaCityAdsDaycountHandler, AreaTop3AdsCountHandler, BlacklistHandler}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("realtime-app")
      val ssc = new StreamingContext(new SparkContext(sparkConf),Seconds(5))
      val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)
    val adsLogDstream: DStream[AdsLog] = recordDstream.map { record =>
      val adsLogString: String = record.value()
      val adsLogArray: Array[String] = adsLogString.split(" ")
      AdsLog(adsLogArray(0).toLong, adsLogArray(1), adsLogArray(2), adsLogArray(3).toLong, adsLogArray(4).toLong)
    }

    //需求 五  黑名单
    val filteredAdslogDstream: DStream[AdsLog] = BlacklistHandler.checkBlackList(ssc.sparkContext,adsLogDstream)

    //更新点击量
    BlacklistHandler.updateUserAdsCount(filteredAdslogDstream)


    //需求六  每天每地区每城市每广告的点击量
    val areaCityAdsDaycountDstream: DStream[(String, Long)] = AreaCityAdsDaycountHandler.handle(filteredAdslogDstream: DStream[AdsLog] ,ssc.sparkContext)

//      recordDstream.foreachRDD{rdd=>
//        println(rdd.map(_.value()) .collect().mkString("\n"))
//      }
    //需求七
    AreaTop3AdsCountHandler.handle(areaCityAdsDaycountDstream)



    ssc.start()
    ssc.awaitTermination()

  }

}
