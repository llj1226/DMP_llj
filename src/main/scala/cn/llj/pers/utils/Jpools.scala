package cn.llj.pers.utils

import cn.llj.pers.config.configHelper
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object Jpools {

  /**
    * 初始化一个redis连接池
    */
  private lazy val jedisPool = new JedisPool(configHelper.redisHost, configHelper.redisPort)

  def getJedis(dbIndex: Int = 0) = {
    val jedis = jedisPool.getResource
    jedis.select(dbIndex)
    jedis
  }
}
