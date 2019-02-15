package cn.llj.pers.config

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc.config.DBs

/**
  * 解析配置文件  application.conf
  */
object configHelper {
  private lazy val load:Config=ConfigFactory.load()

  /**
    * 解析spark.serializer
    * @param args
    */
  val seralizer: String = load.getString("spark.serializer")
  /**
    * 日志所在路径
    */
  val logPath=load.getString("dmp.log.path")
  
  val parquetOutPath: String = load.getString("dmp.parquet.path")

  val url: String = load.getString("db.default.url")
  val user:String=load.getString("db.default.user")
  val password:String=load.getString("db.default.password")
  val driver=load.getString("db.default.driver")
  val table:String=load.getString("db.default.table")
  //地域报表表名
  val areaRtp:String=load.getString("db.rpt.area")

  //app字典的路径
  val appDictPath: String = load.getString("app.dict.path")

  val redisHost: String = load.getString("db.redis.host")
  val redisPort: Int = load.getInt("db.redis.port")
  //scalike  加载MySql配置参数
  private val setup: Unit = DBs.setup()

}
