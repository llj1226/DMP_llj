package cn.llj.pers.report

import java.util.Properties

import akka.japi.Option.Some
import cn.llj.pers.config.configHelper
import cn.llj.pers.utils.rptKpiHelper
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 媒体报表分析
  */
object RptAppAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("媒体报表分析")
    conf.set("spark.serializer",configHelper.seralizer)
    val sc = new SparkContext(conf)
    /**
      * 字典文件
      * 数据处理完了要记得收集到driver端,这样广播出去的数据才是全量的数据
      */
    val appdict = sc.textFile(configHelper.appDictPath)
      .map(_.split("\t",-1))
      .filter(_.length>=5)
      .map(arr=>(arr(4),arr(1)))
      .collectAsMap()

    //把数据广播出去
    val appbct = sc.broadcast(appdict)

    val sQLContext = new SQLContext(sc)
    val dataFrame = sQLContext.read.parquet(configHelper.parquetOutPath)
    val result = dataFrame.map(row => {
      var appName: String = row.getAs("appname")
      val appId: String = row.getAs("appid")

      /**
        * 如果appName为空，则去字典文件中找(根据appid去找)*/
      if (StringUtils.isEmpty(appName)) {
        if (StringUtils.isNotEmpty(appId)) {
          appName = appbct.value.getOrElse(appId, appId)
        } else {
          appName = "未知"
        }
      }
      //appname,kpi
      (appName, rptKpiHelper.rptKpi(row))
    }).reduceByKey((list1, list2) => list1.zip(list2).map(tp => tp._1 + tp._2))


    /**
      * 将结果存入到mysql中
      */
    import sQLContext.implicits._
    val props = new Properties()
    props.setProperty("driver",configHelper.driver)
    props.setProperty("user",configHelper.user)
    props.setProperty("password",configHelper.password)
    result.map(tp=>(tp._1,tp._2(0),tp._2(1),tp._2(2),tp._2(3),tp._2(4),tp._2(5),tp._2(6),tp._2(7),tp._2(8)))
      .toDF("appName","adRawReq","adEffReq","adReq"
      ,"adRtpReq","adSuccReq","adCost","adExpense","adShow","adClick")
        .write.jdbc(configHelper.url,"rpt_app_analysis_llj",props)

    sc.stop()
  }
}
