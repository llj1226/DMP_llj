package cn.llj.pers.report

import java.util.Properties

import cn.llj.pers.config.configHelper
import cn.llj.pers.utils.{Jpools, rptKpiHelper}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 媒体分析， redis 替换广播变量
  */
object RptAppAnalysisRedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("媒体报表分析")
    conf.set("spark.serializer",configHelper.seralizer)

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val dataFrame = sQLContext.read.parquet(configHelper.parquetOutPath)

    /**
      * 计算指标结果
      */


    /*val result=dataFrame.mapPartitions(iter=>{
      val jedis = Jpools.getJedis(4)
      iter.map(row=>{
        var appName = row.getAs[String]("appname")
        val appId = row.getAs[String]("appid")

    /**
      * 判断appName是否为空 ，如果为空，则根据appId去redis找*/
    val resultIterator = if(StringUtils.isEmpty(appName)){
      if(StringUtils.isNotEmpty(appId)){
        appName=jedis.hget("appdict",appId)
        if(StringUtils.isEmpty(appName)) appName=appId
      }else appName="未知"
    }
        (appName,rptKpiHelper.rptKpi(row))
      })
      jedis.close()
      resultIterator
    })*/
    val result = dataFrame.mapPartitions(iter => {
      val jedis = Jpools.getJedis(4)

      val resultIterator = iter.map(row => {
        var appName = row.getAs[String]("appname")
        val appId = row.getAs[String]("appid")

        /**
          * 判断appName是否为空，如果为空，则根据appId去redis找
          **/
        if (StringUtils.isEmpty(appName)) {
          if (StringUtils.isNotEmpty(appId)) {
            appName = jedis.hget("appdict", appId)
            if (StringUtils.isEmpty(appName)) appName = appId
          } else appName = "未知"
        }
        // (appname, kpi)
        (appName, rptKpiHelper.rptKpi(row))
      })

      jedis.close()
      resultIterator
    }).reduceByKey((list1,list2)=>list1.zip(list2).map(tp=>tp._1+tp._2))

    /**
      * 将结果存入到mysql中
      */
    import sQLContext.implicits._
    val props = new Properties()
    props.setProperty("driver",configHelper.driver)
    props.setProperty("user",configHelper.user)
    props.setProperty("password",configHelper.password)

    result.map(tp=>(tp._1,tp._2(0),tp._2(1),tp._2(2),tp._2(3),tp._2(4),tp._2(5),tp._2(6),tp._2(7),tp._2(8)))
      .toDF(
        "appName", "adRawReq", "adEffReq", "adReq",
        "adRtbReq", "adSuccReq", "adCost", "adExpense", "adShow", "adClick"
      ).write.jdbc(configHelper.url,"rpt_app_analysis_2_llj",props)

    sc.stop()

  }
}
