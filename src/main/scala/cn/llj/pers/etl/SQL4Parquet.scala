package cn.llj.pers.etl

import cn.llj.pers.beans.Log
import cn.llj.pers.config.configHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQL4Parquet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("测试读取parquet文件")
      .setMaster("local[*]")
      .set("spark.serializer",configHelper.seralizer)
    val sc = new SparkContext(conf)
    val sQlContext = new SQLContext(sc)
    sQlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    val dataFrame = sQlContext.read.parquet("f:/dmp/parquetv2")
    dataFrame.show()
    sc.stop()
  }
}
