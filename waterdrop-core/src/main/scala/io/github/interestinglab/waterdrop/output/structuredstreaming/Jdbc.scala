package io.github.interestinglab.waterdrop.output.structuredstreaming

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util.Properties

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.output.utils.StructuredUtils
import io.github.interestinglab.waterdrop.utils.JdbcConnectionPoll
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.JavaConversions._

class Jdbc extends BaseStructuredStreamingOutput{

  var config = ConfigFactory.empty()

  val jdbcPrefix = "jdbc."

  var tableName: String = _

  var sql: String = _

  var connection: Connection = _

  var jdbcConnProps: Properties = new Properties()

  override def setConfig(config: Config): Unit = this.config = config


  override def getConfig(): Config = config


  override def checkConfig(): (Boolean, String) = {

    config.hasPath("url") && config.hasPath("user") && config.hasPath("table")
    config.hasPath("password") && config.hasPath("driver") && config.hasPath("output_sql") match {
      case true => (true,"")
      case false => (false,"please specify [url] and [user] and [table] and [password] and [driver] and [output_sql]")
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

    TypesafeConfigUtils.hasSubConfig(config,jdbcPrefix) match {
      case true => {
        TypesafeConfigUtils.extractSubConfig(config, jdbcPrefix, false)
          .entrySet()
          .foreach(entry => {
            val key = entry.getKey
            val value = String.valueOf(entry.getValue.unwrapped())
            jdbcConnProps.put(key, value)
          })
      }
      case false => {}
    }

    tableName = config.getString("jdbc.table")
    sql = config.getString("output_sql")
  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    connection = JdbcConnectionPoll.getPoll(jdbcConnProps).getConnection
    true
  }


  override def process(row: Row): Unit = {
    val ps = connection.prepareStatement(sql)
    setPrepareStatement(row,ps)
    ps.execute()
    ps.close()
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
