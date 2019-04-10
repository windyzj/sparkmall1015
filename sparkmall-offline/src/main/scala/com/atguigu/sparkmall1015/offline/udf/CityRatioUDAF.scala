package com.atguigu.sparkmall1015.offline.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class CityRatioUDAF  extends  UserDefinedAggregateFunction{


  //输入参数类型  udafxxxx(city_name)    StringType
  override def inputSchema: StructType = new StructType(Array(StructField("city_name",StringType)))
  //存储器类型  map (String,long)=> 在这个分组内的 存储各个城市的点击次数    Long=>  在这个分组内的 存储所有城市的点击总数
  override def bufferSchema: StructType = new StructType(Array(StructField("city_map",MapType(StringType,LongType)),StructField("total_click",LongType) ))
  //返回值 输出类型   String
  override def dataType: DataType = StringType

  // 校验   如果相同的输入有相同的返回值 则返回true
  override def deterministic: Boolean = true

  // 对存储器进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=new HashMap[String,Long]
    buffer(1)=0L
  }

  //更新存储器 ，根据穿入值更新存储器
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityName: String = input.getString(0)
    val cityMap: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val totalClick: Long = buffer.getLong(1)

    //更新组内各城市计数清单
    buffer(0)=  cityMap+(cityName->(cityMap.getOrElse(cityName,0L)+1))
     //更新组内所有城市点击总数
    buffer(1)=totalClick+1L
  }

  //合并存储器 ， 不同executor的存储器 进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cityMap1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
    val totalClick1: Long = buffer1.getLong(1)

    val cityMap2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
    val totalClick2: Long = buffer2.getLong(1)

    buffer1(0)=  cityMap1.foldLeft(cityMap2){case (cityMap2,(cityName,count))=>
        cityMap2+ (cityName-> (cityMap2.getOrElse(cityName,0L)+count))
      }

    buffer1(1)=totalClick1+totalClick2


  }

  //把存储器中的结果，进行展示，结果类型必须和上面定义的输出类型一致\
  // 展示效果：北京21.2%，天津13.2%，其他65.6%
  override def evaluate(buffer: Row): String = {
    val cityMap: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val totalClick: Long = buffer.getLong(1)

    //  1 先排序   //  2 截取前2名
    val top2CityCount: List[(String, Long)] = cityMap.toList.sortWith(_._2>_._2).take(2)

    // 3 算百分比
    var cityInfoTop2List: List[CityInfo] = top2CityCount.map { case (cityName, count) =>
      val cityRatio: Double = Math.round(count.toDouble * 1000 / totalClick) / 10.toDouble
      CityInfo(cityName, cityRatio)
    }


    // 4 制作“其他”
    var otherRatio=100D
    for (cityInfo <- cityInfoTop2List ) {
      otherRatio -= cityInfo.ratio
      otherRatio=Math.round(otherRatio * 10) / 10.toDouble
    }
    cityInfoTop2List= cityInfoTop2List :+ CityInfo("其他",otherRatio)


    // 5 拼接成为字符串
    cityInfoTop2List.mkString(",")

  }

  case class CityInfo(name:String ,ratio:Double){
    override def toString: String = name+":"+ratio+"%"
  }
}
