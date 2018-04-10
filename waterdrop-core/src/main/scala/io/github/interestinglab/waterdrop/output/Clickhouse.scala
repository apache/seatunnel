package io.github.interestinglab.waterdrop.output

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection}

import scala.collection.immutable.HashMap

class Clickhouse (var config : Config) extends BaseOutput(config) {

  var schema: Map[String, String] = new HashMap[String, String]()
  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("host", "table", "database", "fields");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length > 0) {
      (false, "please specify " + nonExistsOptions.map("[" + _._1 + "]").mkString(", ") + " as non-empty string")
    } else if (config.hasPath("username") && !config.hasPath("password") || config.hasPath("password") && !config.hasPath("username")) {
      (false, "please specify username and password at the same time")
    } else {

      val jdbcLink = String.format("jdbc:clickhouse://%s/%s", config.getString("host"),
        config.getString("database"))
      val balanced = new BalancedClickhouseDataSource(jdbcLink)

      val conn = config.hasPath("username") match {
        case true => balanced.getConnection(config.getString("username"), config.getString("password"))
        case false => balanced.getConnection
      }

      this.schema = getSchema(conn, config.getString("table"))
      (true, "")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
  }

  override def process(df: DataFrame): Unit = {
    df.foreachPartition { iter =>
      while (iter.hasNext) {
        val item = iter.next()
        println(item)
        println("hello world")
      }
    }
  }

  private def getSchema(conn: ClickHouseConnection, table: String) : Map[String, String] = {
    val sql = String.format("desc %s", table)
    val resultSet = conn.createStatement.executeQuery(sql)
    var schema = new HashMap[String, String]()
    while (resultSet.next()) {
      schema += (resultSet.getString(1) -> resultSet.getString(2))
    }
    schema
  }
}


