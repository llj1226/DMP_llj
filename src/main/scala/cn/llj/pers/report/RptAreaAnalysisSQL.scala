package cn.llj.pers.report

import java.util.Properties

import cn.llj.pers.config.configHelper
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域报表   sparkSql实现
  */
object RptAreaAnalysisSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sql实现地域报表")
    conf.setMaster("local[*]")
    conf.set("spark.serializer",configHelper.seralizer)
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //读取数据
    val dataFrame = sQLContext.read.parquet(configHelper.parquetOutPath)
    //对dataframe进行sql操作
    dataFrame.registerTempTable("log")
    //自定义udf函数
    sQLContext.udf.register("myif",(boolean:Boolean)=>if(boolean)1 else 0)
    //对log的数据进行查询统计
    val result = sQLContext.sql(
      """
        |select provincename, cityname,
        |sum(myif(requestmode=1 and processnode>=1)) adRawReq,
        |sum(if(requestmode=1 and processnode>=2, 1, 0)) adEffReq,
        |sum(if(requestmode=1 and processnode=3, 1, 0)) adReq,
        |
        |sum(if(iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0, 1, 0)) adRtbReq,
        |sum(if(iseffective=1 and isbilling=1 and iswin=1, 1, 0)) adWinReq,
        |
        |sum(if(requestmode=2 and iseffective=1, 1, 0)) adShow,
        |sum(if(requestmode=3 and iseffective=1, 1, 0)) adClick,
        |
        |sum(if(iseffective=1 and isbilling=1 and iswin=1, winprice/1000, 0)) winprice,
        |sum(if(iseffective=1 and isbilling=1 and iswin=1, adpayment/1000, 0)) adpayment
        |
        |from log group by provincename, cityname
      """.stripMargin)
    val props = new Properties()
    props.setProperty("driver",configHelper.driver)
    props.setProperty("user",configHelper.user)
    props.setProperty("password",configHelper.password)

    result.write.mode(SaveMode.Overwrite).jdbc(configHelper.url,"rpt_are_analysis_sql_llj",props)

    sc.stop()
  }
}
