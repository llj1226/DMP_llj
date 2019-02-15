package cn.llj.pers.report

import java.util.Properties

import cn.llj.pers.config.configHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计日志文件中各省市数据分布情况
  * 利用SparkSQL实现
  */
object ProCityAnalysis {
  def main(args: Array[String]): Unit = {
    //套路代码
    val conf = new SparkConf()
    conf.setAppName("统计各省市数据分布情况")
    conf.setMaster("local[*]")
    conf.set("spark.serializer",configHelper.seralizer)

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //读取数据parquet文件
   val dataFrame = sQLContext.read.parquet(configHelper.parquetOutPath)
    //注册表
    dataFrame.registerTempTable("log")

    //按照需求进行统计:各省市数据分布情况
    val result = sQLContext.sql(
      """
select provincename,cityname,count(*) cnt
from log
group by provincename,cityname
      """.stripMargin
    )
    //结果保存  json格式
    result.coalesce(1).write.json("f:/dmp/json")

    //写mysql
    val props = new Properties()
    props.setProperty("driver",configHelper.driver)
    props.setProperty("user",configHelper.user)
    props.setProperty("password",configHelper.password)
    result.write.jdbc(configHelper.url,configHelper.table,props)
    //关闭
    sc.stop()
  }
}
