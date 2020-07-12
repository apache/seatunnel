package io.github.interestinglab.waterdrop.receiver

import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

/**
  * redis receiver
  * @author bfd-mingbei.xu
  */
class RedisReceiver (host: String,password: String, prefKey: String, queue: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Serializable with Logging {

  var cluster: JedisCluster = _
  def createRedisCluster() = {
    println("init redis pool begin")
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(200)
    poolConfig.setMaxIdle(200)
    poolConfig.setMaxWaitMillis(2000)
    val nodes = new util.LinkedHashSet[HostAndPort]
    host.split(",").foreach(node => {
      val str = node.split(":")
      nodes.add(new HostAndPort(str(0), Integer.valueOf(str(1))))
    })
    if(!StringUtils.isEmpty(password)){
      cluster = new JedisCluster(nodes, 5000, 5000, 5,password, poolConfig)
    }else{
      cluster = new JedisCluster(nodes, 5000, 5000, 5, poolConfig)
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
          val outPut = cluster.blpop(1, getKey(queue))
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
      //关闭连接池
      if (cluster != null) {
        cluster.close()
      }
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host, e)
      case t: Exception =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
    println("get Data Thread end")
  }


  class Job() extends Runnable {
    override def run(): Unit = {

    }
  }


  def getKey(key: String): String = {
    if (StringUtils.isBlank(key)) key
    else if (StringUtils.isBlank(prefKey)) key
    else if (key.startsWith(prefKey + ":")) key
    else prefKey + ":" + key
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
