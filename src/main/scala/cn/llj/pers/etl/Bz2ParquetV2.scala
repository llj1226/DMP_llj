package cn.llj.pers.etl
import cn.llj.pers.beans.Log
import cn.llj.pers.config.configHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 日志格式转换 第二种实现方式
  */
object Bz2ParquetV2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("将bz2转换成parquet文件")
    conf.setMaster("local[*]")
    //RDD | worker -> work
    conf.set("spark.serializer",configHelper.seralizer)
    //注册自定义类的序列化方式
    conf.registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)
    val sQlContext = new SQLContext(sc)
    sQlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    val rowLog = sc.textFile(configHelper.logPath)
    val filtered = rowLog.map(line => line.split(",", -1))
      .filter(_.length >= 85)
    val logRDD = filtered.map(Log(_))
    val dataFrame = sQlContext.createDataFrame(logRDD)
    dataFrame.write.partitionBy("provincename","cityname").parquet("f:/dmp/parquetv4")
    sc.stop()
  }
}
