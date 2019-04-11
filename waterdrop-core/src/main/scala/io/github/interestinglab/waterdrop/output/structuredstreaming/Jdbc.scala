package io.github.interestinglab.waterdrop.output.structuredstreaming

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.output.utils.StructuredUtils
import io.github.interestinglab.waterdrop.utils.JdbcConnectionPoll
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class Jdbc extends BaseStructuredStreamingOutput{

  var config = ConfigFactory.empty()

  val jdbcPrefix = "jdbc."

  var tableName: String = _

  var poll: JdbcConnectionPoll = _

  var connection: Connection = _

  override def setConfig(config: Config): Unit = this.config = config


  override def getConfig(): Config = config


  override def checkConfig(): (Boolean, String) = {

    config.hasPath("url") && config.hasPath("user") && config.hasPath("table")
    config.hasPath("password") && config.hasPath("driver") match {
      case true => (true,"")
      case false => (false,"please specify [url] and [user] and [table] and [password] and [driver]")
    }

  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConf = ConfigFactory.parseMap(
      Map(
        jdbcPrefix + "driverClassName" -> config.getString("driver"),
        jdbcPrefix + "url" -> config.getString("url"),
        jdbcPrefix + "username" -> config.getString("user"),
        jdbcPrefix + "password" -> config.getString("password"),
        jdbcPrefix + "table" -> config.getString("table"),
        jdbcPrefix + "initialSize" -> "1",
        jdbcPrefix + "minIdle" -> "1",
        jdbcPrefix + "maxActive" -> "10",
        jdbcPrefix + "maxWait" -> "10000",
        jdbcPrefix + "timeBetweenEvictionRunsMillis" -> "60000",
        jdbcPrefix + "minEvictableIdleTimeMillis" -> "300000",
        jdbcPrefix + "testWhileIdle" -> "true",
        jdbcPrefix + "testOnBorrow" -> "true",
        jdbcPrefix + "testOnReturn" -> "false",
        "jdbc_output_mode" -> "replace",
        "trigger_type" -> "default"
      )
    )
    config = config.withFallback(defaultConf)

    val properties = new Properties()

    TypesafeConfigUtils.hasSubConfig(config,jdbcPrefix) match {
      case true => {
        TypesafeConfigUtils.extractSubConfig(config, jdbcPrefix, false)
          .entrySet()
          .foreach(entry => {
            val key = entry.getKey
            val value = String.valueOf(entry.getValue.unwrapped())
            properties.put(key, value)
          })
      }
      case false => {}
    }

    poll = JdbcConnectionPoll.getPoll(properties)

    tableName = config.getString("jdbc.table")

  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    connection = poll.getConnection
    true
  }


  override def process(row: Row): Unit = {
    val fields = row.schema.fieldNames
    val values = ArrayBuffer[String]()
    fields.foreach(_ => values += "?")
    val fieldStr = fields.mkString("(",",",")")
    val valueStr = values.mkString("(",",",")")
    val sql = config.getString("jdbc_output_mode") match {
      case "replace" => s"REPLACE INTO $tableName$fieldStr VALUES$valueStr"
      case "insert ignore" => s"INSERT IGNORE INTO $tableName$fieldStr VALUES$valueStr"
      case _ => throw new RuntimeException("unknown output_mode,only support [replace] and [insert ignore]")
    }
    val ps = connection.prepareStatement(sql)
    setPrepareStatement(row,ps)
    ps.execute()
  }

  private def setPrepareStatement(row: Row,ps: PreparedStatement): Unit = {

    for (i <- 0 until row.size) {
      row.get(i) match {
        case v: Int => ps.setInt(i + 1, v)
        case v: Long => ps.setLong(i + 1, v)
        case v: Float => ps.setFloat(i + 1, v)
        case v: Double => ps.setDouble(i + 1, v)
        case v: String => ps.setString(i + 1, v)
        case v: Timestamp => ps.setTimestamp(i + 1, v)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }


  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    var writer = df.writeStream
      .outputMode(config.getString("streaming_output_mode"))
      .foreach(this)

    writer = StructuredUtils.setCheckpointLocation(writer, config)
    StructuredUtils.writeWithTrigger(config, writer)
  }


}
