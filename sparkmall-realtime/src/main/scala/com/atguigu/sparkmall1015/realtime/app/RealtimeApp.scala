package com.atguigu.sparkmall1015.realtime.app

import com.atguigu.sparkmall1015.common.util.MyKafkaUtil
import com.atguigu.sparkmall1015.realtime.BlacklistHandler
import com.atguigu.sparkmall1015.realtime.bean.AdsLog
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
    val filteredAdslogDstream: DStream[AdsLog] = BlacklistHandler.checkBlackList(ssc.sparkContext,adsLogDstream)

    //更新点击量
    BlacklistHandler.updateUserAdsCount(filteredAdslogDstream)



//      recordDstream.foreachRDD{rdd=>
//        println(rdd.map(_.value()) .collect().mkString("\n"))
//      }
    ssc.start()
    ssc.awaitTermination()

  }

}
