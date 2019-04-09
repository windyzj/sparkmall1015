package com.atguigu.sparkmall1015.mock.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean):String ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复
    var str=""
    if(canRepeat){
      val buffer = new ListBuffer[Int]
      while(buffer.size<amount){
        val randonNum: Int = fromNum+ new Random().nextInt(toNum-fromNum+1)
        buffer+= randonNum
      }
      str=buffer.mkString(delimiter)
    }else{
      val set = new mutable.HashSet[Int]
      while(set.size<amount){
        val randonNum: Int = fromNum+ new Random().nextInt(toNum-fromNum+1)
        set+= randonNum
      }
      str=set.mkString(delimiter)
    }
    str
  }

  def main(args: Array[String]): Unit = {
    println(multi(1, 5, 3, ",", false))
  }


}
