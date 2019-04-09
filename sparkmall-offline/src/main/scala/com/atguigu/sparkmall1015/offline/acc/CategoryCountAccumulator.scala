package com.atguigu.sparkmall1015.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryCountAccumulator extends  AccumulatorV2[String,mutable.HashMap[String ,Long]]{

  private var categoryCountMap = new mutable.HashMap[String ,Long]()

  // 判空
  override def isZero: Boolean = categoryCountMap.isEmpty
  //拷贝
  override def copy(): AccumulatorV2[String, mutable.HashMap[String ,Long]] = {
    val accumulator = new CategoryCountAccumulator
    accumulator.categoryCountMap ++= categoryCountMap
    accumulator
  }

  override def reset(): Unit = {
    categoryCountMap.clear()
  }

  //累加
  override def add(key: String): Unit = {
    //原值加一
    categoryCountMap(key)= categoryCountMap.getOrElse(key,0L)+1L
  }

   //   合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String ,Long]]): Unit = {
   //两个map之间进行合并
    val otherMap: mutable.HashMap[String, Long] = other.value
    categoryCountMap= categoryCountMap.foldLeft(otherMap){case (otherMap,(key,count))=>
        otherMap(key)=otherMap.getOrElse(key,0L)+count
        otherMap
    }


  }

  override def value: mutable.HashMap[String ,Long] = {
    this.categoryCountMap
  }
}
