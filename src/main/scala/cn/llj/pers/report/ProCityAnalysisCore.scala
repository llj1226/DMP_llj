package cn.llj.pers.report

import java.util.Properties

import cn.llj.pers.beans.RptProvinceCityResult
import cn.llj.pers.config.configHelper
import com.google.gson.Gson
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scalikejdbc.{DB, SQL}

/**
  * 统计各省市数据分布情况
  * Spark Core
  */
object ProCityAnalysisCore {
  def main(args: Array[String]): Unit = {
    //套路代码
    val conf = new SparkConf()
    conf.setAppName("统计各省市数据分布情况core")
    conf.setMaster("local[*]")
    conf.set("spark.serializer",configHelper.seralizer)

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val dataFrame = sQLContext.read.parquet(configHelper.parquetOutPath)
    //Core -> reduceByKey -> RDD[(K=(省，城市)，1)]
    val result = dataFrame.map(row => {
      //省
      val pName = row.getAs[String]("provincename")

      //地市
      val cityName = row.getAs[String]("cityname")

      ((pName, cityName), 1)
    }).reduceByKey(_ + _)

    //保存结果为json

  //json 输出方式一
 /*   result.map(
      tp=>{
        val gson = new Gson()
        gson.toJson(RptProvinceCityResult(tp._1._1,tp._1._2,tp._2))
      }
    ).saveAsTextFile("f:/dmp/json2")
//json输出方式二
    import sQLContext.implicits._
    val resultDF = result.map(tp => (tp._1._1, tp._1._2, tp._2)).toDF("pname", "cname", "cnt")
    resultDF.write.json("f:/dmp/json3")

    //保存结果到mysql  方式一
    val props = new Properties()
    props.setProperty("driver",configHelper.driver)
    props.setProperty("user",configHelper.user)
    props.setProperty("password",configHelper.password)
    resultDF.write.jdbc(configHelper.url,configHelper.table,props)*/
    //mysql  方式二
    //1.加载配置文件
    result.foreachPartition(partition=>{
      DB.localTx{
        implicit session=>
          partition.foreach(tp=>{
            SQL("insert into rpt_province_2_llj(pname,cname,cnt) values(?,?,?)")
              .bind(tp._1._1,tp._1._2,tp._2).update().apply()
          })
      }
    })
    sc.stop()
  }
}
