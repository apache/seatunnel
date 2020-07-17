package io.github.interestinglab.waterdrop.input.sparkstreaming.redis

import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

class RedisReceiver(redisInfo: RedisInfo) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Serializable with Logging {

  var cluster: JedisCluster = _

  def createRedisCluster(): Unit = {
    println("init redis pool begin")
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(redisInfo.maxTotal)
    poolConfig.setMaxIdle(redisInfo.maxIdle)
    poolConfig.setMaxWaitMillis(redisInfo.maxWaitMillis)
    val nodes = new util.LinkedHashSet[HostAndPort]
    redisInfo.host.split(",").foreach(node => {
      val str = node.split(":")
      nodes.add(new HostAndPort(str(0), Integer.valueOf(str(1))))
    })
    if (!StringUtils.isEmpty(redisInfo.password)) {
      cluster = new JedisCluster(nodes, redisInfo.connectionTimeout, redisInfo.soTimeout, redisInfo.maxAttempts, redisInfo.password, poolConfig)
    } else {
      cluster = new JedisCluster(nodes, redisInfo.connectionTimeout, redisInfo.soTimeout, redisInfo.maxAttempts, poolConfig)
    }
    println("init redis pool end")
  }

  override def onStart() {
    //init redis
    createRedisCluster()
    // Start the thread that receives data over a connection
    new Thread("redis Receiver") {
      override def run() {
        receive()
      }
    }.start()

  }

  private def receive() {
    println("get Data Thread begin")
    try {
      while (!isStopped) {
        try {
          import scala.collection.JavaConversions._
          val outPut = cluster.blpop(1, getKey(redisInfo.queue))
          if (outPut != null && !outPut.isEmpty) {
            for (str <- outPut) {
              if (StringUtils.isNotEmpty(str)) {
                store(str)
              }
            }
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            println("======>>printStackTraceStr Exception: " + e.getClass + "\n==> " + e.toString + "\n==>data= redis receive error")
        }
      }
      if (cluster != null) {
        cluster.close()
      }
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + redisInfo.host, e)
      case t: Exception =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
    println("get Data Thread end")
  }


  def getKey(key: String): String = {
    if (StringUtils.isBlank(key) || StringUtils.isBlank(redisInfo.prefKey) || key.startsWith(redisInfo.prefKey + ":")) {
      key
    } else {
      redisInfo.prefKey + ":" + key
    }
  }

  override def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
    if (cluster != null) {
      cluster.close()
    }
    println("get Data Thread error")
  }

}
