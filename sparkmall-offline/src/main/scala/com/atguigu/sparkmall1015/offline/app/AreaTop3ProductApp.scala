package com.atguigu.sparkmall1015.offline.app

import java.util.Properties

import com.atguigu.sparkmall1015.common.util.PropertiesUtil
import com.atguigu.sparkmall1015.offline.udf.CityRatioUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaTop3ProductApp {

  def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("area_top3").setMaster("local[*]")
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val properties: Properties = PropertiesUtil.load("config.properties")

    val database=  properties.getProperty("hive.database");

    sparkSession.udf.register("city_remark",new CityRatioUDAF)

    sparkSession.sql("use "+database);

    sparkSession.sql(" select  ci.area,ci.city_name, click_product_id   from  user_visit_action uv   join city_info ci on uv.city_id=ci.city_id  where uv.click_product_id >0").createOrReplaceTempView("tmp_area_product")

    sparkSession.sql(" select area ,click_product_id, count(*) ct , city_remark(city_name) remark from  tmp_area_product  group by area ,click_product_id ").createOrReplaceTempView("tmp_area_prod_count")

    sparkSession.sql("select *  ,  row_number()over(partition by area order by ct desc ) rk    from    tmp_area_prod_count") .createOrReplaceTempView("tmp_area_prod_rank")

     sparkSession.sql(" select area,pi.product_name,tr.ct,tr.remark  from  tmp_area_prod_rank tr ,product_info pi where rk<=3 and  pi.product_id=tr.click_product_id").write.format("jdbc")
       .option("url", properties.getProperty("jdbc.url"))
       .option("user", properties.getProperty("jdbc.user"))
       .option("password", properties.getProperty("jdbc.password"))
       .option("dbtable", "area_prod_rank").mode(SaveMode.Append).save()




  }

}
