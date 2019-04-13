package com.atguigu.sparkmall1015.realtime.handler

import org.apache.spark.streaming.dstream.DStream

object AreaTop3AdsCountHandler {

//  需求 七
//    每天各地区 top3 热门广告
//  数据来源 根据 需求六  得到    areaCityAdsDaycountDstream: DStream[(areaCityAdsDay  ：String, count Long)]
//  //调整结构把 city去掉 重新组成key value
//  areaCityAdsDaycountDstream.map{(areaCityAdsDay,count)=>(areaAdsDay,count)}
//  //利用reducebykey的 新的汇总值
//  areaCityAdsDaycountDstream: DStream[(areaAdsDay  ：String, count Long)]
//  =>map >  DStream[(datekey,(area,(ads,count)))]
//  =>groupbykey=>DStream[(datekey,iterable[(area,(ads,count))])]
//  =>.map
//  {
//    iterable[(area,(ads,count)].groupby=>
//
//    Map[area ,iterable[(area,(ads,count)]]
//      .map =>iterable[(ads,count)]
//    // 排序  截取前三
//    iterable[(ads,count)].tolist.sortwith .take(3)
//  }
//  DStream[datekey ,Map[area,Map[ads,count]]]
//  hmset (datekey ,Map[area,Map[ads,count]]  )    count 是什么count    count =>把 每天各地区各城市各广告的点击流量  去掉城市 聚合一次可得

//  hmset (datekey ,Map[area,JSON]  )       map=>  json
//  hmset (key ,Map[String,String]  )
  def handle(areaCityAdsDaycountDstream:  DStream[(String, Long)]): Unit ={
      //1231231231

   // /12312312331123
  }

}
