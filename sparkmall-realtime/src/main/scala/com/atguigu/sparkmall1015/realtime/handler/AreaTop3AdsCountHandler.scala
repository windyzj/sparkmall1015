package com.atguigu.sparkmall1015.realtime.handler

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall1015.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._
import redis.clients.jedis.Jedis
object AreaTop3AdsCountHandler {

//  需求 七
//    每天各地区 top3 热门广告
  //1 、重新计算点击量   ：调整结构把 city去掉 重新组成key value   进行reducebykey
//    数据来源 根据 需求六  得到    areaCityAdsDaycountDstream: DStream[(areaCityAdsDay  ：String, count Long)]

//  // 1.1 调整结构把 city去掉 重新组成key value
    // 1.2 reducebykey  得到 每天每地区每广告的点击量   // count 是什么count    count =>把 每天各地区各城市各广告的点击流量  去掉城市 聚合一次可得
        //  areaCityAdsDaycountDstream.map{(areaCityAdsDay,count)=>(areaAdsDay,count)}
      //利用reducebykey的 新的汇总值
  //2 调整格式 保存到redis中   DStream[(areaAdsDay  ：String, count Long)] =>
  //  数据 areaCityAdsDaycountDstream: DStream[(areaAdsDay  ：String, count Long)] =>  DSTream (datekey ,Map[area,JSON]  )
  // 2.1 重新分组  daykey大组
          // 调整结构 key 拆开  变成   DStream[(datekey,(area,(ads,count)))]
          //按照datekey进行分组 =>groupbykey  =》  DStream[(datekey,iterable[(area,(ads,count))])]
  //  2.2 在分地区的小组
  //  =>.map
//  {
//    iterable[(area,(ads,count)].groupby=>   //分小组
  //2.3 小组内做整顿  1.把小组的地区字段去掉，冗余 2 排序 3 截取前三
//    Map[area ,iterable[(area,(ads,count)]]
//      .map =>iterable[(ads,count)]  //1.把小组的地区字段去掉
//       iterable[(ads,count)].tolist.sortwith .take(3) // 排序  截取前三
//  }
    //2.4 把整顿好的小组 转成json
//  DStream (datekey ,Map[area,Map[ads,count]]  )
  //  DStream (datekey ,Map[area,JSON]  )
  //3 把最后结果保存到redis中

//  hmset (key ,Map[String,String]  )
  def handle(areaCityAdsDaycountDstream:  DStream[(String, Long)]): Unit ={
    //1 、重新计算点击量   ：调整结构把 city去掉 重新组成key value   进行reducebykey
    //    数据来源 根据 需求六  得到    areaCityAdsDaycountDstream: DStream[(areaCityAdsDay  ：String, count Long)]

    //  // 1.1 调整结构把 city去掉 重新组成key value
    // 1.2 reducebykey  得到 每天每地区每广告的点击量   // count 是什么count    count =>把 每天各地区各城市各广告的点击流量  去掉城市 聚合一次可得
    val areaAdsDaycountDstream: DStream[(String, Long)] = areaCityAdsDaycountDstream.map { case (areaCityAdsDay, count) =>
      val areaCityAdsDayArray: Array[String] = areaCityAdsDay.split(":")
      val area: String = areaCityAdsDayArray(0)
      val ads: String = areaCityAdsDayArray(2)
      val date: String = areaCityAdsDayArray(3)
      val areaAdsDatekey = area + ":" + ads + ":" + date;
      (areaAdsDatekey, count)
    }.reduceByKey(_ + _)

    //2 调整格式 保存到redis中   DStream[(areaAdsDay  ：String, count Long)] =>
    //  数据 areaCityAdsDaycountDstream: DStream[(areaAdsDay  ：String, count Long)] =>  DSTream (datekey ,Map[area,JSON]  )
    // 2.1 重新分组  daykey大组
    // 2.1 调整结构 key 拆开  变成   DStream[(datekey,(area,(ads,count)))]
    //2.2按照datekey进行分组 =>groupbykey  =》  DStream[(datekey,iterable[(area,(ads,count))])]
    val areaAdsCountGroupbyDateDstream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsDaycountDstream.map { case (areaAdsDatekey, count) =>
      val areaAdsDayArray: Array[String] = areaAdsDatekey.split(":")
      val area: String = areaAdsDayArray(0)
      val ads: String = areaAdsDayArray(1)
      val date: String = areaAdsDayArray(2)
      (date, (area, (ads, count)))
    }.groupByKey()

    //
    //  =>.map
    //  {
    // 大组整顿
    val  areaAdsTop3CountGroupByDateDstream: DStream[(String,Map[String, String])] = areaAdsCountGroupbyDateDstream.map { case (date, areaAdsCountItr) =>
     // 2.2 在分地区的小组  // iterable[(area,(ads,count)].groupby=>   //分小组
      val adsCountGroupbyArea: Map[String, Iterable[(String, (String, Long))]] = areaAdsCountItr.groupBy { case (area, (ads, count)) => area }

      //2.3 小组内做整顿  1.把小组的地区字段去掉，冗余 2 排序 3 截取前三
      //    Map[area ,iterable[(area,(ads,count)]]
      //      .map =>iterable[(ads,count)]  //1.把小组的地区字段去掉
      //       iterable[(ads,count)].tolist.sortwith .take(3) // 排序  截取前三
      //  }
      val top3AdsCountJsonGroupbyAreaMap: Map[String, String] = adsCountGroupbyArea.map { case (area, adsCountPerAreaItr) =>
        //2.3.1.把小组的地区字段去掉
        val adsCountItr: Iterable[(String, Long)] = adsCountPerAreaItr.map { case (area, (ads, count)) => (ads, count) }
        // 2.3.2  排序  截取前三
        val adsCountTop3List: List[(String, Long)] = adsCountItr.toList.sortWith(_._2 > _._2).take(3)

        //2.3.3  转json    //由于fastjson gson jackson  只能转java对象   //  JSON.toJSONString(adsCountTop3List)
        //所以要是用scala专用json工具  json4s
        val top3AdsjsonString: String = JsonMethods.compact(JsonMethods.render(adsCountTop3List))
        (area, top3AdsjsonString)
      }
      (date, top3AdsCountJsonGroupbyAreaMap)
    }

    //3 保存到redis中
    areaAdsTop3CountGroupByDateDstream.foreachRDD {rdd =>

      rdd.foreachPartition{dateItr=>
       val jedis: Jedis = RedisUtil.getJedisClient
        import collection.JavaConversions._
        dateItr.foreach { case (date, top3AdsCountJsonGroupbyAreaMap) =>
          val key="top3_ads_per_day:"+date
          jedis.hmset(key,top3AdsCountJsonGroupbyAreaMap)  //每天增加一个key
        }
        jedis.close()

      }
    }



  }

}
