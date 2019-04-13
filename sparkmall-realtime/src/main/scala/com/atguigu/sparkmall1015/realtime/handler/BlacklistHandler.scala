package com.atguigu.sparkmall1015.realtime.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.sparkmall1015.common.util.RedisUtil
import com.atguigu.sparkmall1015.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlacklistHandler {

  /**
    * 更新用户每天点击广告计数
    *
    * @param adsLogDstream
    */
  def updateUserAdsCount(adsLogDstream: DStream[AdsLog]): Unit = {
    adsLogDstream.foreachRDD { rdd =>

      //  设计 Redis 键值： user_ads_daycount    hash结构  field   uid:ads:date   value: count
      //      rdd.foreach{adslog=>
      //         val jedis: Jedis = RedisUtil.getJedisClient  /// 建立连接太频繁
      //         val key="user_ads_daycount"
      //         val date = new SimpleDateFormat("yyyy-MM-dd").format( new Date(adslog.ts))
      //         val field=adslog.uid+":"+adslog.adsId+":"+date
      //         jedis.hincrBy(key,field,1L)
      //      }
      rdd.foreachPartition { adsLogItr => //在executor中执行
        val jedis: Jedis = RedisUtil.getJedisClient
        val key = "user_ads_daycount"
        val blackListKey = "blacklist"
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        adsLogItr.foreach { adslog =>

          val date = formatter.format(new Date(adslog.ts))
          val field = adslog.uid + ":" + adslog.adsId + ":" + date
          //更新 用户当日点击量
          jedis.hincrBy(key, field, 1L)
          val countStr: String = jedis.hget(key, field)
          //达到100点击量 保存进黑名单
          if (countStr.toLong >= 100) {
            jedis.sadd(blackListKey, adslog.uid.toString)
          }

        }
        jedis.close()

      }
    }
  }

  /**
    * 二、 根据黑名单对用户进行过滤
  过滤依据  jedis.smembers()   黑名单集合
  dstream.filter(adslog =>  !jedis.sismember(adslog.uid) )
    * @param sparkContext
    * @param adsLogDstream
    * @return
    */
  def checkBlackList(sparkContext: SparkContext, adsLogDstream: DStream[AdsLog]): DStream[AdsLog] = {

    //以下操作会每行连接一次 做一次redis查询 因为一个时间周期内blacklist是不会变化的所有反复查询会浪费性能
    //    val filterDstream: DStream[AdsLog] = adsLogDstream.filter { adslog =>
    //      val jedis: Jedis = RedisUtil.getJedisClient
    //      !jedis.sismember("blacklist", adslog.uid.toString)
    //    }

    //以下操作 只执行一次 driver中  会造成黑名单无法更新
    //    val jedis: Jedis = RedisUtil.getJedisClient
    //    val blackList: util.Set[String] = jedis.smembers("blacklist")
    //    val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blackList)

      val filteredDstream: DStream[AdsLog] = adsLogDstream.transform { rdd =>
       //每时间间隔执行一次 driver中
      val jedis: Jedis = RedisUtil.getJedisClient
      val blackList: util.Set[String] = jedis.smembers("blacklist")
      jedis.close()
      //每个固定周期从redis中取到最新的黑名单 通过广播变量发送给executor
      val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blackList)

      //executor 根据广播变量的黑名单进行过滤
      val filterRDD: RDD[AdsLog] = rdd.filter { adsLog =>
        !blacklistBC.value.contains(adsLog.uid.toString) //executor中执行
      }
      filterRDD

    }
    filteredDstream

  }

}
