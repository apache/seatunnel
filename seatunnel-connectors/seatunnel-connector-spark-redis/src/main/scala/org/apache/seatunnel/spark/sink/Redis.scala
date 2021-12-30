package org.apache.seatunnel.spark.sink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import redis.clients.jedis.JedisCluster


/**
 * @FileDescription
 * @Create 2021-12-07 13:07
 *
 */
class Redis extends SparkBatchSink with Logging{

  var redisCfg: Map[String, String] = _
  val redisPrefix = "redis."
  var cluster: JedisCluster = _

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {


    data.toDF().show()
  }


  override def checkConfig(): CheckResult = {
    val requiredOptions = List("host", "prefKey", "queue")

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.nonEmpty) {
      val message = " as non-empty string please specify "
      new CheckResult(false, message)
    }else{
      new CheckResult(true, "")
    }

  }


  override def prepare(prepareEnv: SparkEnvironment): Unit = {

    redisCfg = config.entrySet().asScala.map {
      entry => s"$redisPrefix.${entry.getKey}" -> String.valueOf(entry.getValue.unwrapped())
    }.toMap
    redisCfg.foreach( println(_))

  }






}
