package org.apache.seatunnel.spark.redis.common

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, RedisNode}

class SelfAchievedRedisProxyConfig(initialHost: RedisEndpoint) extends RedisConfig(initialHost) {
  override val initialAddr: String = initialHost.host

  override val hosts: Array[RedisNode] = getNodes(initialHost)
  override val nodes: Array[RedisNode] = getNodes(initialHost)

  override def getNodes(initialHost: RedisEndpoint): Array[RedisNode] = {
    Array(RedisNode(initialHost, 0, 16383, 0, 1))
  }
}
