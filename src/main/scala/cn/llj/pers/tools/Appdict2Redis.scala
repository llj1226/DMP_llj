package cn.llj.pers.tools

import cn.llj.pers.config.configHelper
import cn.llj.pers.utils.Jpools
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将字典数据存入到redis中
  */
object Appdict2Redis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("将字典数据存入到redis中")
    conf.set("spark.serializer",configHelper.seralizer)

    val sc = new SparkContext(conf)

    /**
      * 读取数据
      */
    sc.textFile(configHelper.appDictPath)
      .map(line => line.split("\t", -1))
      .filter(_.length >= 5)
      .foreachPartition(patition => {
        // 从池子获取连接
        val jedis = Jpools.getJedis(4)

        /**
          * scala 中的迭代器只能迭代一次
          */
        patition.foreach(arr => {
          jedis.hset("appdict", arr(4), arr(1))
        })

        jedis.close()
      })


    sc.stop()
  }
}
