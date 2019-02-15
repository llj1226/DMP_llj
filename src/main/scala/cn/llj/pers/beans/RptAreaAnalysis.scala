package cn.llj.pers.beans

import java.util.Properties

import cn.llj.pers.config.configHelper
import cn.llj.pers.utils.rptKpiHelper
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RptAreaAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("地域分布")
    conf.setMaster("local[*]")
    conf.set("spark.serializer",configHelper.seralizer)

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //读取数据
    val dataFrame = sQLContext.read.parquet(configHelper.parquetOutPath)
    import sQLContext.implicits._
    //处理数据 =>(K,V)
    val resultDF = dataFrame.map(row => {
      val currentRowKpi = rptKpiHelper.rptKpi(row)
      val pName = row.getAs[String]("provincename")
      val cName = row.getAs[String]("cityname")
      //k=地域(省，市)  V=List[Double](原始请求，有效请求，广告请求，参与竞价数，竞价成功数，广告成本，广告消费，展示量，点击量)
      ((pName, cName), currentRowKpi)
    }).reduceByKey {
      (list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2)
    }.map(tp => RptAreaResult(tp._1._1, tp._1._2, tp._2(0),
      tp._2(1), tp._2(2), tp._2(3), tp._2(4),
      tp._2(5), tp._2(6), tp._2(7), tp._2(8))).toDF()

    //写入到mysql中
    val props = new Properties()
    props.setProperty("driver",configHelper.driver)
    props.setProperty("user",configHelper.user)
    props.setProperty("password",configHelper.password)

    resultDF.write.jdbc(configHelper.url,configHelper.areaRtp,props)



    sc.stop()
  }


}
