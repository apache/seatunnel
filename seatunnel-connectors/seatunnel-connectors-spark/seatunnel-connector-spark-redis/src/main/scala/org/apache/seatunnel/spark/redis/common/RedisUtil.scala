package org.apache.seatunnel.spark.redis.common

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.seatunnel.shade.com.typesafe.config.Config
import org.apache.seatunnel.spark.redis.common.Constants.{AUTH, DB_NUM, HOST, PORT, TIMEOUT}

object RedisUtil {
  def getRedisConfig(isSelfAchieved: Boolean, config: Config): RedisConfig ={
    if(isSelfAchieved) {
      new SelfAchievedRedisProxyConfig(RedisEndpoint(
        host = config.getString(HOST),
        port = config.getInt(PORT),
        auth = if (config.getIsNull(AUTH)) null else config.getString(AUTH),
        dbNum = config.getInt(DB_NUM),
        timeout = config.getInt(TIMEOUT)
      ))
    } else {
      new RedisConfig(RedisEndpoint(
        host = config.getString(HOST),
        port = config.getInt(PORT),
        auth = if (config.getIsNull(AUTH)) null else config.getString(AUTH),
        dbNum = config.getInt(DB_NUM),
        timeout = config.getInt(TIMEOUT)
      ))
    }
  }
}
