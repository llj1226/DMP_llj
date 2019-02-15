package cn.llj.pers.etl

import java.io.File

import cn.llj.pers.beans.LogSchema
import cn.llj.pers.config.configHelper
import cn.llj.pers.utils.NumParse
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * 将bz2转换成parquet日志文件
  */
object Bz2Parquet {
  def main(args: Array[String]): Unit = {
    //sparkCOntext是程序的入口
    val sparkConf = new SparkConf()
    sparkConf.setAppName("将bz2转换成parquet日志文件")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer",configHelper.seralizer)
    val sc = new SparkContext(sparkConf)
    val sQlContext = new SQLContext(sc)
    sQlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //读取数据
    val rawLog = sc.textFile(configHelper.logPath)
    //按照需求进行格式转换(parquet <= row + schema =DataFrame)
    val filtered :RDD[Array[String]]= rawLog.map(line => line.split(",", -1)) //-1表示要切分到最后一个
      .filter(_.length >= 85)
    val rowRDD = filtered.map(arr => Row(
      arr(0),
      NumParse.toInt(arr(1)),
      NumParse.toInt(arr(2)),
      NumParse.toInt(arr(3)),
      NumParse.toInt(arr(4)),
      arr(5),
      arr(6),
      NumParse.toInt(arr(7)),
      NumParse.toInt(arr(8)),
      NumParse.toDouble(arr(9)),
      NumParse.toDouble(arr(10)),
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      NumParse.toInt(arr(17)),
      arr(18),
      arr(19),
      NumParse.toInt(arr(20)),
      NumParse.toInt(arr(21)),
      arr(22),
      arr(23),
      arr(24),
      arr(25),
      NumParse.toInt(arr(26)),
      arr(27),
      NumParse.toInt(arr(28)),
      arr(29),
      NumParse.toInt(arr(30)),
      NumParse.toInt(arr(31)),
      NumParse.toInt(arr(32)),
      arr(33),
      NumParse.toInt(arr(34)),
      NumParse.toInt(arr(35)),
      NumParse.toInt(arr(36)),
      arr(37),
      NumParse.toInt(arr(38)),
      NumParse.toInt(arr(39)),
      NumParse.toDouble(arr(40)),
      NumParse.toDouble(arr(41)),
      NumParse.toInt(arr(42)),
      arr(43),
      NumParse.toDouble(arr(44)),
      NumParse.toDouble(arr(45)),
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54),
      arr(55),
      arr(56),
      NumParse.toInt(arr(57)),
      NumParse.toDouble(arr(58)),
      NumParse.toInt(arr(59)),
      NumParse.toInt(arr(60)),
      arr(61),
      arr(62),
      arr(63),
      arr(64),
      arr(65),
      arr(66),
      arr(67),
      arr(68),
      arr(69),
      arr(70),
      arr(71),
      arr(72),
      NumParse.toInt(arr(73)),
      NumParse.toDouble(arr(74)),
      NumParse.toDouble(arr(75)),
      NumParse.toDouble(arr(76)),
      NumParse.toDouble(arr(77)),
      NumParse.toDouble(arr(78)),
      arr(79),
      arr(80),
      arr(81),
      arr(82),
      arr(83),
      NumParse.toInt(arr(84))
    ))
    val logDataFrame = sQlContext.createDataFrame(rowRDD,LogSchema.schema)
    //先判断一下输出目录是否存在，如果存在则删除
    //val file = new File("f:/dmp/parquet")
    /*if(file.exists()){
      FileUtils.deleteDirectory(file)
    }*/
    val hadoopConfiguration = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfiguration)
    val path = new Path(configHelper.parquetOutPath)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    //存储文件
    logDataFrame.write.parquet(configHelper.parquetOutPath)
    sc.stop()
  }
}
