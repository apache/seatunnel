package io.github.interestinglab.waterdrop.input.batch

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._
import com.redislabs.provider.redis._

/**
 * 使用spark.redis读取redis中数据，转化成DataFrame
 * Usage:
 * host = "192.168.1.100"
 * key_pattern = "*key*"
 * partition = 3
 * db_num = 0
 * result_table_name = "table_name"
 * */
class Redis extends BaseStaticInput {
  var config: Config = ConfigFactory.empty()

  val defaultPort: Int = 6379
  val defaultDb: Int = 0
  val defaultPatition: Int = 3

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    val hasTableName = config.hasPath("result_table_name") || config.hasPath("table_name")
    val hasRedisHost = config.hasPath("host")
    val hasKeys = config.hasPath("key_pattern")
    val hasRedisPassword=config.hasPath("auth")

    config match {
      case _ if !hasTableName =>
        (false, "please specify [result_table_name] as non-empty string")
      case _ if !hasRedisHost =>
        (false, "please specify [host] as non-empty string")
      case _ if !hasRedisPassword =>
        (false, "please specify [auth] as non-empty string")
      case _ if !hasKeys =>
        (false, "please specify [key_pattern] as non-empty string, multiple key patterns separated by ','")
      case _ => (true, "")

    }
  }

  /**
   * 参数项设置
   *
   * @param spark sparkSession上下文
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }

  /**
   * 读取redis中数据，转化成DataFrame
   *
   * @param spark sparkSession上下文
   * @return 返回输入的数据集
   */
  override def getDataset(spark: SparkSession): Dataset[Row] = {

    val regTable = if(config.hasPath("result_table_name")) config.getString("result_table_name") else config.getString("table_name")
    val auth=config.getString("auto")
    val host = config.getString("host")
    val port = if(config.hasPath("port")) config.getInt("port") else defaultPort
    val keyPattern = config.getString("key_pattern")
    val partition = if(config.hasPath("partition")) config.getInt("partition") else defaultPatition
    val dbNum = if(config.hasPath("db_num")) config.getInt("db_num") else defaultDb

    // 通过keys从redis中获取数据回来并组合成dataset
    val redisConfig = new RedisConfig(new RedisEndpoint(host = host, port = port, auth=auth,dbNum = dbNum))
    val stringRDD = spark.sparkContext.fromRedisKV(keyPattern, partition)(
    import spark.implicits._
    val ds = stringRDD.toDF("raw_key", "raw_message")
    ds.createOrReplaceTempView(s"$regTable")
    ds
  }
}
